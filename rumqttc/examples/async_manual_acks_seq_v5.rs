use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::mqttbytes::QoS;
use tokio::{task, time, sync::Notify};
use std::sync::{Arc, Mutex};

use rumqttc::v5::{AsyncClient, Event, EventLoop, ManualAck, ManualAckReason, MqttOptions};
use std::error::Error;
use std::time::Duration;

fn create_conn() -> (AsyncClient, EventLoop) {
    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions
        .set_keep_alive(Duration::from_secs(5))
        .set_manual_acks(true)
        .set_clean_start(false)
        .set_session_expiry_interval(5.into())
        ;

    AsyncClient::new(mqttoptions, 10)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    println!("");
    println!(">>>>>>>>>>> Create broker connection, do not ack broker publishes!!!");

    // create mqtt connection with clean_session = false and manual_acks = true
    let (client, mut eventloop) = create_conn();

    // subscribe example topic
    client
        .subscribe("hello/world", QoS::AtLeastOnce)
        .await
        .unwrap();

    task::spawn(async move {
        // send some messages to example topic and disconnect
        requests(&client).await;
        client.disconnect().await.unwrap()
    });

    // get subscribed messages without acking
    loop {
        let event = eventloop.poll().await;
        match &event {
            Ok(v) => {
                println!("Event = {v:?}");
            }
            Err(e) => {
                println!("Error = {e:?}");
                break;
            }
        }
    }

    println!("");
    println!(">>>>>>>>>>> Create new broker connection to get unack packets again!!!");

    // create new broker connection
    let (client, mut eventloop) = create_conn();

    // ACK packets with different delays
    let waits = vec![200, 180, 160, 140, 120, 100, 80, 60, 40, 20, 10];
    let n_wait = waits.len();
    let mut n_ack = 0;
    let acks: Arc<Mutex<Vec<(u16, ManualAck, bool)>>> = Arc::new(Mutex::new(Vec::new()));
    let notify = Arc::new(Notify::new());
    let n0 = notify.clone();

    let c = client.clone();
    let l = acks.clone();
    tokio::spawn(async move {
        loop {
            notify.notified().await;
            // drain all acks which are ready
            loop {
                // if no acks are ready, break
                if l.lock().unwrap().is_empty() {
                    break;
                }
                // Check if first ack is ready, if so, send it
                if l.lock().unwrap()[0].2 {
                    let (_pkid, ack, _ready) = l.lock().unwrap().remove(0);
                    c.manual_ack(ack).await.unwrap();
                } else {
                    // if first ack is not ready, break
                    break;
                }
            }
        }
    });
    while let Ok(event) = eventloop.poll().await {
        println!("{event:?}");

        if let Event::Incoming(packet) = event {
            let publish = match packet {
                Packet::Publish(publish) => publish,
                _ => continue,
            };
            // this time we will ack incoming publishes.
            // Its important not to block notifier as this can cause deadlock.
            let c = client.clone();
            let wait = waits[n_ack].clone();
            let acks_c = acks.clone();
            let n = n0.clone();
            tokio::spawn(async move {
                let ack = c.get_manual_ack(&publish);
                let ppkid = publish.pkid;
                acks_c.lock().unwrap().push((ppkid, ack, false));

                time::sleep(Duration::from_millis(wait)).await;

                let mut acks_d = acks_c.lock().unwrap();
                if let Some(pos) = acks_d.iter().position(|(pkid, _, _)| *&ppkid == *pkid) {
                    acks_d[pos].1.set_reason(ManualAckReason::Success);
                    acks_d[pos].1.set_reason_string("Testing no error".to_string().into());
                    acks_d[pos].2 = true;
                    n.notify_one();
                }
            });
            n_ack = (n_ack + 1) % n_wait;
        }
    }

    Ok(())
}

async fn requests(client: &AsyncClient) {
    for i in 1..=10 {
        client
            .publish("hello/world", QoS::AtLeastOnce, false, vec![1; i])
            .await
            .unwrap();

        time::sleep(Duration::from_millis(100)).await;
    }
}
