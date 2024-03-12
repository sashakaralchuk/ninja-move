use std::env;

use kafka::producer::{Producer as KafkaProducer, Record};

pub struct Producer {
    producer: KafkaProducer,
}

impl Producer {
    pub fn new_and_connect() -> Self {
        let brokers = env::var("KAFKA_BROKERS_URLS").unwrap()
            .split(",")
            .map(|url| url.to_string())
            .collect::<Vec<String>>();
        let producer = KafkaProducer::from_hosts(brokers).create().unwrap();
        Self { producer }
    }

    pub fn send_all(
        &mut self, topic: &'static str, messages: &Vec<String>,
    ) -> Result<(), String> {
        let records_to_send = messages.iter()
            .map(|x| Record::from_value(topic, x.as_bytes()))
            .collect::<Vec<Record<'_, (), &[u8]>>>();
        match self.producer.send_all(&records_to_send) {
            Ok(_) => return Ok(()),
            Err(error) => Err(format!("error: {}", error))
        }
    }
}
