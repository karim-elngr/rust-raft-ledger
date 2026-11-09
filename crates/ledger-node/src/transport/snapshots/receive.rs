use super::*;
use ledger_replica::{SNAPSHOT_MAX_MANIFEST, SnapshotRange, SnapshotReceiver};
use prost::Message as _;

struct Download {
    client: RaftPeerClient<Channel>,
    cluster: WireGroup,
    from: u64,
    to: u64,
    generation: [u8; 32],
    stats: SnapshotTransferStats,
    lease: OfferLease,
}

impl Download {
    async fn read(
        &mut self,
        file: Option<u32>,
        offset: u64,
        length: u32,
    ) -> Result<SnapshotChunk, Status> {
        let mut request = Request::new(SnapshotRead {
            version: self.cluster.version(),
            cluster: self.cluster.bytes().to_vec(),
            from: self.from,
            to: self.to,
            generation: self.generation.to_vec(),
            file,
            offset,
            length,
        });
        self.stats.rpc_message_bytes += request.get_ref().encoded_len() as u64;
        request.set_timeout(CHUNK_TIMEOUT);
        let mut client = self
            .client
            .clone()
            .max_decoding_message_size(SNAPSHOT_CHUNK_BYTES + 128);
        let chunk = timeout(CHUNK_TIMEOUT, client.read_snapshot(request))
            .await
            .map_err(|_| Status::deadline_exceeded("snapshot chunk timed out"))??
            .into_inner();
        self.stats.rpc_message_bytes += chunk.encoded_len() as u64;
        if chunk.data.len() > length as usize || (!chunk.end && chunk.data.len() != length as usize)
        {
            return Err(Status::data_loss("incorrect snapshot chunk length"));
        }
        self.lease.activity();
        Ok(chunk)
    }

    async fn manifest(&mut self) -> Result<Vec<u8>, Status> {
        let mut bytes = Vec::new();
        loop {
            let left = SNAPSHOT_MAX_MANIFEST - bytes.len();
            if left == 0 {
                return Err(Status::resource_exhausted("snapshot manifest too large"));
            }
            let count = left.min(SNAPSHOT_CHUNK_BYTES) as u32;
            let chunk = self.read(None, bytes.len() as u64, count).await?;
            bytes.extend_from_slice(&chunk.data);
            if chunk.end {
                return Ok(bytes);
            }
        }
    }

    async fn history(&mut self, after: u64) -> Result<Vec<Vec<u8>>, Status> {
        let mut request = Request::new(SnapshotHistoryRead {
            version: self.cluster.version(),
            cluster: self.cluster.bytes().to_vec(),
            from: self.from,
            to: self.to,
            generation: self.generation.to_vec(),
            after,
        });
        self.stats.rpc_message_bytes += request.get_ref().encoded_len() as u64;
        request.set_timeout(CHUNK_TIMEOUT);
        let page = timeout(CHUNK_TIMEOUT, self.client.read_snapshot_history(request))
            .await
            .map_err(|_| Status::deadline_exceeded("snapshot history timed out"))??
            .into_inner();
        self.stats.rpc_message_bytes += page.encoded_len() as u64;
        if page.entries.is_empty() || page.entries.len() > 64 {
            return Err(Status::data_loss("invalid snapshot history group"));
        }
        self.lease.activity();
        Ok(page.entries)
    }

    async fn replay_proof(&mut self, base: CheckpointRef) -> Result<Option<[u8; 32]>, Status> {
        let mut request = Request::new(SnapshotReplayRead {
            version: self.cluster.version(),
            cluster: self.cluster.bytes().to_vec(),
            from: self.from,
            to: self.to,
            generation: self.generation.to_vec(),
            base_index: base.index,
            base_term: base.term,
        });
        let request_bytes = request.get_ref().encoded_len() as u64;
        self.stats.rpc_message_bytes += request_bytes;
        self.stats.replay_proof_bytes += request_bytes;
        request.set_timeout(CHUNK_TIMEOUT);
        let proof = match timeout(CHUNK_TIMEOUT, self.client.read_snapshot_replay(request)).await {
            Ok(Ok(response)) => response.into_inner(),
            // An optional optimization may be absent or take longer than a
            // single RPC. The mandatory physical protocol has bounded ranges.
            Err(_) => return Ok(None),
            Ok(Err(status))
                if matches!(
                    status.code(),
                    tonic::Code::Unimplemented
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Cancelled
                ) =>
            {
                return Ok(None);
            }
            Ok(Err(status)) => return Err(status),
        };
        let response_bytes = proof.encoded_len() as u64;
        self.stats.rpc_message_bytes += response_bytes;
        self.stats.replay_proof_bytes += response_bytes;
        self.lease.activity();
        if proof.digest.is_empty() {
            return Ok(None);
        }
        proof
            .digest
            .try_into()
            .map(Some)
            .map_err(|_| Status::data_loss("invalid snapshot replay proof length"))
    }
}

impl Snapshots {
    pub(in crate::transport) async fn receive(&self, message: Message) -> Result<(), Status> {
        let reference = reference(&message)?;
        // Membership is fixed for this Raft group. Reject a wrong envelope
        // before reading its manifest or requesting any files.
        if !self.permits_message(&message)
            || message.get_snapshot().get_metadata().get_conf_state()
                != self.resources.history.membership()
        {
            return Err(Status::invalid_argument(
                "snapshot Raft membership differs from recipient",
            ));
        }
        let _admission = self
            .incoming
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("another snapshot download is active"))?;
        let lease = OfferLease::new();
        tokio::select! {
            biased;
            _ = stopped(self.stop.clone()) => Err(Status::unavailable("voter is stopped")),
            _ = lease.ended() => Err(Status::deadline_exceeded("snapshot transfer idle")),
            result = self.download(message, reference, lease.clone()) => result,
        }
    }

    async fn download(
        &self,
        message: Message,
        reference: CheckpointRef,
        lease: OfferLease,
    ) -> Result<(), Status> {
        let channel = self
            .channels
            .get(&message.from)
            .ok_or_else(|| Status::permission_denied("unknown snapshot sender"))?;
        let mut download = Download {
            client: RaftPeerClient::new(channel.clone())
                .max_decoding_message_size(MAX_ENVELOPE_BYTES),
            cluster: self.cluster,
            from: self.node,
            to: message.from,
            generation: reference.digest,
            stats: SnapshotTransferStats::default(),
            lease,
        };
        let (reply, rx) = oneshot::channel();
        self.resources
            .events
            .try_send(SnapshotEvent::Prepare {
                from: message.from,
                term: message.term,
                index: reference.index,
                reply,
            })
            .map_err(|_| Status::unavailable("snapshot owner busy or stopped"))?;
        let reuse = rx
            .await
            .map_err(|_| Status::unavailable("voter stopped"))?
            .map_err(|_| Status::failed_precondition("stale snapshot offer"))?;
        let manifest = download.manifest().await?;
        download.stats.manifest_bytes = manifest.len() as u64;
        let files = self.resources.files.clone();
        let receiver = self
            .io(move || files.receive(reference, &manifest, reuse))
            .await?;
        let (image, history) = self
            .receive_image(&mut download, receiver, reference)
            .await?;
        let (admitted, receipt) = oneshot::channel();
        self.resources
            .events
            .try_send(SnapshotEvent::Received(Box::new(ReceivedSnapshot {
                message,
                image,
                history,
                stats: download.stats,
                admitted,
            })))
            .map_err(|_| Status::unavailable("snapshot owner busy or stopped"))?;
        match receipt.await {
            Ok(true) => Ok(()),
            _ => Err(Status::failed_precondition("voter declined snapshot")),
        }
    }

    async fn receive_image(
        &self,
        download: &mut Download,
        receiver: SnapshotReceiver,
        reference: CheckpointRef,
    ) -> Result<(ImportedCheckpoint, HistoryStage), Status> {
        let (ready, history) = if let Some(base) = receiver.replay_base()
            && let Some(proof) = download.replay_proof(base).await?
        {
            let (mut receiver, history) = self.stage_history(download, receiver, reference).await?;
            let (receiver, history, image) = self
                .io(move || {
                    let image = receiver.replay(&history, proof)?;
                    Ok((receiver, history, image))
                })
                .await?;
            if let Some(image) = image {
                download.stats.replay_base_index = Some(base.index);
                return Ok((image, history));
            }
            // A damaged or obsolete local seal can still use the physical image.
            (self.fetch_files(download, receiver).await?, history)
        } else {
            // Preserve the physical protocol's file-first resume behavior.
            let ready = self.fetch_files(download, receiver).await?;
            self.stage_history(download, ready, reference).await?
        };
        let image = self.io(move || ready.finish()).await?;
        Ok((image, history))
    }

    async fn fetch_files(
        &self,
        download: &mut Download,
        mut receiver: SnapshotReceiver,
    ) -> Result<SnapshotReceiver, Status> {
        let total_file_bytes = receiver.total_file_bytes();
        let (mut receiver, mut next) = self
            .io(move || {
                let next = receiver.next_range()?;
                Ok((receiver, next))
            })
            .await?;
        while let Some(range) = next {
            let SnapshotRange {
                file,
                offset,
                length,
            } = range;
            let chunk = download.read(Some(file), offset, length).await?;
            download.stats.file_bytes += chunk.data.len() as u64;
            (receiver, next) = self
                .io(move || {
                    receiver.write_chunk(range, &chunk.data)?;
                    let next = receiver.next_range()?;
                    Ok((receiver, next))
                })
                .await?;
        }
        download.stats.reused_or_staged_file_bytes = total_file_bytes
            .checked_sub(download.stats.file_bytes)
            .ok_or_else(|| Status::data_loss("snapshot file payload exceeds manifest total"))?;
        Ok(receiver)
    }

    async fn stage_history(
        &self,
        download: &mut Download,
        mut receiver: SnapshotReceiver,
        reference: CheckpointRef,
    ) -> Result<(SnapshotReceiver, HistoryStage), Status> {
        let store = self.resources.history.clone();
        let (mut history, mut after) = self
            .io(move || {
                let stage = store.begin(reference)?;
                let after = stage.resume_after()?;
                Ok((stage, after))
            })
            .await?;
        while after < reference.index {
            let encoded = download.history(after).await?;
            download.stats.history_entries += encoded.len() as u64;
            download.stats.history_bytes +=
                encoded.iter().map(|entry| entry.len() as u64).sum::<u64>();
            (receiver, history, after) = self
                .io(move || {
                    history.append_encoded(&encoded)?;
                    receiver.record_activity()?;
                    let advanced = history.resume_after()?;
                    if advanced <= after {
                        return Err(ReplicaError::Invalid(
                            "snapshot history made no progress".into(),
                        ));
                    }
                    Ok((receiver, history, advanced))
                })
                .await?;
        }
        let history = self
            .io(move || {
                history.finish()?;
                Ok(history)
            })
            .await?;
        Ok((receiver, history))
    }
}
