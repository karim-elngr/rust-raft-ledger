use crate::event;
use ledger_export::{EventPublisher, EventRecord, ExportError, Identity, MAX_EVENTS, Result};
use prost::Message;
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use std::sync::Mutex;

/// Each ledger is assigned an explicit partition. Availablity of its synchronous
/// replica set and topic replication/min.insync.replicas policy belong to Kafka.
pub struct KafkaPublisher {
    destination: String,
    topic: String,
    partition: i32,
    producer: FutureProducer,
    publishing: Mutex<()>,
}
impl KafkaPublisher {
    /// Connection and authentication settings may be supplied without weakening
    /// delivery acknowledgments, retry ordering, or TLS certificate verification.
    pub fn new(
        brokers: &str,
        topic: &str,
        partition: i32,
        options: &[(String, String)],
    ) -> Result<Self> {
        let mut seeds: Vec<_> = brokers.split(',').map(str::trim).collect();
        if seeds.iter().any(|s| {
            s.is_empty()
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:[]".contains(&b))
        }) {
            return Err(ExportError::Invalid(
                "expected comma-separated Kafka broker host[:port] addresses".into(),
            ));
        }
        if partition < 0
            || topic.is_empty()
            || topic.len() > 249
            || matches!(topic, "." | "..")
            || !topic
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(ExportError::Invalid(
                "invalid Kafka topic or partition".into(),
            ));
        }
        seeds.sort_unstable();
        seeds.dedup();
        let brokers = seeds.join(",");
        let mut config = ClientConfig::new();
        for (key, value) in options {
            match key.as_str() {
                "security.protocol"
                | "ssl.ca.location"
                | "ssl.certificate.location"
                | "ssl.key.location"
                | "ssl.key.password"
                | "sasl.mechanism"
                | "sasl.mechanisms"
                | "sasl.username"
                | "sasl.password" => (),
                "message.timeout.ms"
                    if value
                        .parse::<u32>()
                        .is_ok_and(|ms| (1..=300_000).contains(&ms)) => {}
                _ => {
                    return Err(ExportError::Invalid(format!(
                        "unsupported Kafka option or value for {key}"
                    )));
                }
            }
            config.set(key, value);
        }
        config
            .set("bootstrap.servers", &brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("max.in.flight.requests.per.connection", "1")
            .set("linger.ms", "0")
            .set("allow.auto.create.topics", "false")
            .set("message.max.bytes", "16777216");
        if !options.iter().any(|(k, _)| k == "message.timeout.ms") {
            config.set("message.timeout.ms", "10000");
        }
        // Configuration failures may contain a supplied secret. Report only a safe description.
        let producer = config.create().map_err(|_| {
            ExportError::Invalid("Kafka producer configuration could not be initialized".into())
        })?;
        Ok(Self {
            destination: format!("kafka://{brokers}/{topic}/{partition}"),
            topic: topic.into(),
            partition,
            producer,
            publishing: Mutex::new(()),
        })
    }
}
impl EventPublisher for KafkaPublisher {
    fn destination(&self) -> &str {
        &self.destination
    }
    fn publish(&self, id: &Identity, events: &[EventRecord]) -> Result<()> {
        if events.len() > MAX_EVENTS
            || events
                .iter()
                .any(|e| e.position.index == 0 || e.position.slot == u16::MAX)
            || events.windows(2).any(|w| w[0].position >= w[1].position)
        {
            return Err(ExportError::Invalid(
                "expected a bounded ordered Kafka event page".into(),
            ));
        }
        let _order = self
            .publishing
            .lock()
            .map_err(|_| ExportError::Unavailable("Kafka publish worker interrupted".into()))?;
        for e in events {
            let record = event::event(id, e);
            let payload = record.encode_to_vec();
            let msg = FutureRecord::to(&self.topic)
                .partition(self.partition)
                .key(&record.event_id)
                .payload(&payload);
            let pending = self
                .producer
                .send_result(msg)
                .map_err(|(err, _)| ExportError::Unavailable(err.to_string()))?;
            // Await the delivery report on the dedicated export thread. A failed
            // record may already be visible; never send its successor first.
            let delivery = futures_executor::block_on(pending)
                .map_err(|_| {
                    ExportError::Unavailable(
                        "Kafka producer stopped before delivery confirmation".into(),
                    )
                })?
                .map_err(|(err, _)| ExportError::Unavailable(err.to_string()))?;
            if delivery.partition != self.partition || delivery.offset < 0 {
                return Err(ExportError::Unavailable(
                    "Kafka returned incomplete delivery confirmation".into(),
                ));
            }
        }
        Ok(())
    }
}
