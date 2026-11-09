use ledger_export::{
    EventPublisher, ExportWorkers, LedgerFeed, NoopArchive, NoopPublisher, RecoveryArchive,
    RunningWorkers,
};
#[cfg(feature = "kafka")]
use ledger_export_adapters::KafkaPublisher;
use ledger_export_adapters::PosixArchive;
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ExportConfig {
    pub(super) archive: ArchiveConfig,
    publisher: PublisherConfig,
}

#[derive(Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ArchiveConfig {
    #[default]
    Noop,
    Posix {
        path: PathBuf,
    },
}

#[derive(Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PublisherConfig {
    #[default]
    Noop,
    #[cfg(feature = "kafka")]
    Kafka {
        brokers: String,
        topic: String,
        #[serde(default)]
        partition: i32,
        #[serde(default)]
        options: std::collections::BTreeMap<String, String>,
    },
}

impl ExportConfig {
    #[cfg(feature = "kafka")]
    pub(super) fn resolve_kafka_paths(&mut self, base: &Path) -> crate::Result<()> {
        let PublisherConfig::Kafka { options, .. } = &mut self.publisher else {
            return Ok(());
        };
        for key in [
            "ssl.ca.location",
            "ssl.certificate.location",
            "ssl.key.location",
        ] {
            if let Some(value) = options.get_mut(key) {
                if value.is_empty() {
                    return Err(format!("empty Kafka certificate path for {key}").into());
                }
                *value = base
                    .join(&*value)
                    .into_os_string()
                    .into_string()
                    .map_err(|_| format!("Kafka certificate path for {key} must be UTF-8"))?;
            }
        }
        Ok(())
    }

    pub(crate) fn start(
        self,
        data: &Path,
        feed: Arc<dyn LedgerFeed>,
    ) -> crate::Result<(Arc<ExportWorkers>, RunningWorkers)> {
        let archive: Arc<dyn RecoveryArchive> = match self.archive {
            ArchiveConfig::Noop => Arc::new(NoopArchive),
            ArchiveConfig::Posix { path } => {
                Arc::new(PosixArchive::for_replica(&path, data, &feed.identity()?)?)
            }
        };
        let publisher: Arc<dyn EventPublisher> = match self.publisher {
            PublisherConfig::Noop => Arc::new(NoopPublisher),
            #[cfg(feature = "kafka")]
            PublisherConfig::Kafka {
                brokers,
                topic,
                partition,
                options,
            } => Arc::new(KafkaPublisher::new(
                &brokers,
                &topic,
                partition,
                &options.into_iter().collect::<Vec<_>>(),
            )?),
        };
        let workers = Arc::new(ExportWorkers::open(
            &data.join("export"),
            feed,
            Some(archive),
            Some(publisher),
        )?);
        let running = workers.start(Duration::from_millis(100))?;
        Ok((workers, running))
    }
}

#[cfg(all(test, feature = "kafka"))]
mod tests {
    use super::*;

    #[test]
    fn kafka_pem_paths_are_relative_to_the_config_file() {
        let mut settings: ExportConfig = toml::from_str(
            r#"
            [publisher]
            kind = "kafka"
            brokers = "kafka.example:9093"
            topic = "payments"
            [publisher.options]
            "ssl.ca.location" = "tls/ca.pem"
            "ssl.certificate.location" = "/etc/ledger/client.pem"
            "ssl.key.location" = "keys/client.pem"
            "ssl.key.password" = "literal/value"
        "#,
        )
        .unwrap();
        settings
            .resolve_kafka_paths(Path::new("/srv/config"))
            .unwrap();
        let PublisherConfig::Kafka { options, .. } = settings.publisher else {
            panic!("expected Kafka");
        };
        assert_eq!(options["ssl.ca.location"], "/srv/config/tls/ca.pem");
        assert_eq!(
            options["ssl.certificate.location"],
            "/etc/ledger/client.pem"
        );
        assert_eq!(options["ssl.key.location"], "/srv/config/keys/client.pem");
        assert_eq!(options["ssl.key.password"], "literal/value");
    }
}
