use ledger_core::{Command, Decision, Error, LedgerId, RequestId, ReservationId};
use ledger_export::{EventPublisher, EventRecord, Identity, Position};
use ledger_export_adapters::{KafkaPublisher, v1};
use prost::Message as _;
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    mocking::MockCluster,
};
use std::time::{Duration, Instant};

fn identity() -> Identity {
    Identity {
        ledger: LedgerId(u128::MAX),
        generation: 13,
        asset: "CAD".into(),
        scale: 2,
    }
}
fn events() -> Vec<EventRecord> {
    [7, 9]
        .into_iter()
        .enumerate()
        .map(|(n, req)| EventRecord {
            position: Position {
                index: 42,
                slot: n as u16,
            },
            decision: Decision {
                request: RequestId(req),
                command: Command::Void {
                    reservation: ReservationId(u128::MAX),
                },
                outcome: Err(Error::UnknownReservation(ReservationId(u128::MAX))),
            },
        })
        .collect()
}
fn roundtrip(brokers: &str, topic: &str, failover: Option<std::path::PathBuf>) {
    let publish = KafkaPublisher::new(brokers, topic, 1, &[]).unwrap();
    let batch = events();
    let mut id = identity();
    publish.publish(&id, &batch).unwrap();
    if let Some(dir) = failover {
        std::fs::write(dir.join("acknowledged"), b"7,9").unwrap();
        let deadline = Instant::now() + Duration::from_secs(120);
        while !dir.join("leader-replaced").exists() {
            assert!(
                Instant::now() < deadline,
                "partition did not elect a replacement"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    // Replacement and retry can duplicate delivery. Its business event key survives both.
    id.generation += 1;
    publish.publish(&id, &batch[..1]).unwrap();
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "ledger-export-test")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut partitions = TopicPartitionList::new();
    partitions
        .add_partition_offset(topic, 1, Offset::Beginning)
        .unwrap();
    consumer.assign(&partitions).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut keys = vec![];
    for (slot, req, generation) in [(0, 7, 13), (1, 9, 13), (0, 7, 14)] {
        let msg = loop {
            assert!(Instant::now() < deadline, "consumer timed out");
            if let Some(msg) = consumer.poll(Duration::from_millis(200)) {
                match msg {
                    Ok(record) => break record,
                    Err(e)
                        if e.rdkafka_error_code()
                            == Some(rdkafka::types::RDKafkaErrorCode::BrokerTransportFailure) =>
                    {
                        eprintln!("consumer reconnecting after {e}");
                    }
                    Err(e) => panic!("consumer failed: {e}"),
                }
            }
        };
        let record = v1::DecisionEvent::decode(msg.payload().unwrap()).unwrap();
        assert_eq!(msg.partition(), 1);
        assert_eq!(msg.key().unwrap(), record.event_id.as_bytes());
        assert_eq!(record.schema_version, 1);
        assert_eq!(record.request_id, req.to_string());
        assert_eq!(record.generation, generation.to_string());
        assert_eq!(record.ledger_id, u128::MAX.to_string());
        assert_eq!((record.raft_index, record.slot), (42, slot));
        assert!(
            matches!(record.command,Some(v1::decision_event::Command::VoidReservation(r)) if r.reservation_id == u128::MAX.to_string())
        );
        assert!(
            matches!(record.decision.unwrap().result,Some(ledger_api::v1::decision::Result::Rejected(r)) if r.code == ledger_api::v1::ErrorCode::UnknownReservation as i32)
        );
        keys.push(record.event_id);
    }
    assert_ne!(keys[0], keys[1]);
    assert_eq!(keys[0], keys[2]);
}
#[test]
fn mock_cluster_orders_records_and_replays_stable_business_ids() {
    let cluster = MockCluster::new(3).unwrap();
    cluster.create_topic("ledger-decisions", 2, 3).unwrap();
    roundtrip(&cluster.bootstrap_servers(), "ledger-decisions", None);
}
#[test]
#[ignore = "requires a fresh Kafka topic with at least two partitions"]
fn real_broker_acknowledges_and_consumer_decodes_protobuf() {
    roundtrip(
        &std::env::var("LEDGER_TEST_KAFKA_BROKERS").unwrap(),
        &std::env::var("LEDGER_TEST_KAFKA_TOPIC").unwrap(),
        std::env::var_os("LEDGER_TEST_FAILOVER_DIR").map(Into::into),
    );
}
#[test]
fn unreachable_broker_cannot_acknowledge_delivery() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("unavailable", 1, 1).unwrap();
    cluster.broker_down(1).unwrap();
    let publisher = KafkaPublisher::new(
        &cluster.bootstrap_servers(),
        "unavailable",
        0,
        &[("message.timeout.ms".into(), "600".into())],
    )
    .unwrap();
    assert!(publisher.publish(&identity(), &events()).is_err());
}

#[test]
fn immutable_delivery_and_secret_free_destination_configuration() {
    for key in [
        "acks",
        "enable.idempotence",
        "partitioner",
        "enable.ssl.certificate.verification",
    ] {
        assert!(
            KafkaPublisher::new("broker:9092", "events", 0, &[(key.into(), "false".into())])
                .is_err()
        );
    }
    assert!(KafkaPublisher::new("name:password@broker:9092", "test", 0, &[]).is_err());
}
