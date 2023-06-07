use chrono::{DateTime, Utc};
use influxdb::Client;
use influxdb::InfluxDbWriteable;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use serde_json::Error;
use std::env;
use std::fs::File;
use std::time::Duration;

// example {\"pm25\":106,\"wifi\":{\"ssid\":\"Ravenclaw Tower\",\"ip\":\"192.168.2.103\",\"rssi\":-59}}"
#[derive(Deserialize)]
struct AQIPacket {
    pm25: i32,
}

#[derive(InfluxDbWriteable)]
struct AQIReading {
    pm25: i32,
    time: DateTime<Utc>,
}

#[derive(Deserialize)]
struct StatenConfig {
    mqtt_url: String,
    mqtt_topic: String,
    influx_url: String,
    influx_db: String,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let config_path = match args.len() {
        1 => "/etc/staten_config.json",
        2 => &args[1],
        _ => panic!("Expected one optional argument of config path"),
    };
    let config_file = File::open(config_path).unwrap();
    let config: StatenConfig = serde_json::from_reader(config_file).unwrap();

    let mut mqttoptions = MqttOptions::parse_url(config.mqtt_url).unwrap();
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (mqtt_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    mqtt_client
        .subscribe(config.mqtt_topic, QoS::AtMostOnce)
        .await
        .unwrap();

    let influx_client = Client::new(config.influx_url, config.influx_db);

    while let Ok(notification) = eventloop.poll().await {
        match notification {
            Event::Incoming(Packet::Publish(event)) => {
                let parse: Result<AQIPacket, Error> = serde_json::from_slice(&event.payload);
                if parse.is_ok() {
                    let packet = parse.unwrap();
                    println!("quality: {}", packet.pm25);

                    let to_write = AQIReading {
                        time: Utc::now(),
                        pm25: packet.pm25,
                    }
                    .into_query("aqi");
                    let write_result = influx_client.query(to_write).await;
                    if write_result.is_err() {
                        println!("error writing to influxdb");
                    }
                }
            }
            _ => (),
        }
    }
}
