/*
Copyright (C) 2018 Boyu Yang <yangby@cryptape.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

use amq_protocol::uri::AMQPUri;
use futures::future::Future;
use futures::stream;
use futures::sync::mpsc;
use futures::Stream;
use lapin_futures::channel::{
    BasicConsumeOptions, BasicProperties, BasicPublishOptions, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions, QueuePurgeOptions,
};
use lapin_futures::client::{Client, ConnectionOptions};
use lapin_futures::message::Delivery;
use lapin_futures::types::FieldTable;
use std::io;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::mpsc as std_mpsc;
use std::thread;
use tokio;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio_io::{AsyncRead, AsyncWrite};

pub use super::Payload;

pub const EXCHANGE: &str = "cita";
pub const EXCHANGE_TYPE: &str = "topic";

fn amqp_connect(
    uri: AMQPUri,
    addr: SocketAddr,
    name: &'static str,
    keys: Vec<String>,
    publisher_rx: mpsc::Receiver<Payload>,
    consumer_tx: std_mpsc::Sender<Payload>,
) -> impl Future<Item = (), Error = io::Error> + Send + 'static {
    TcpStream::connect(&addr)
        .and_then(|stream| {
            // connect() returns a future of an AMQP Client
            // that resolves once the handshake is done
            Client::connect(stream, ConnectionOptions::from_uri(uri))
        }).and_then(move |(client, heartbeat)| {
            tokio::spawn(heartbeat.map_err(|_| ()));
            // create_channel returns a future that is resolved
            // once the channel is successfully created
            let publisher = publisher(&client, name, publisher_rx);
            let consumer = consumer(&client, name, keys, consumer_tx);
            publisher
                .select2(consumer)
                .map(|_| ())
                .map_err(|_| Error::new(ErrorKind::Other, "channel closed!"))
        })
}

fn publisher<T>(
    client: &Client<T>,
    name: &'static str,
    publisher_rx: mpsc::Receiver<Payload>,
) -> impl Future<Item = (), Error = io::Error> + Send + 'static
where
    T: AsyncRead + AsyncWrite,
    T: Send + Sync,
    T: 'static,
{
    client.create_channel().and_then(move |channel| {
        channel
            .queue_declare(name, QueueDeclareOptions::default(), FieldTable::new())
            .and_then(move |_| {
                println!("publisher queue declared");
                channel
                    .queue_purge(name, QueuePurgeOptions::default())
                    .and_then(move |_| {
                        println!("publisher queue purged");
                        publisher_rx
                            .for_each(move |(routing_key, msg): (String, Vec<u8>)| {
                                tokio::spawn(
                                    channel
                                        .basic_publish(
                                            EXCHANGE,
                                            &routing_key,
                                            msg,
                                            BasicPublishOptions::default(),
                                            BasicProperties::default(),
                                        ).map(|_| ())
                                        .map_err(|_| ()),
                                )
                            }).map_err(|_| Error::new(ErrorKind::Other, "channel closed!"))
                    })
            })
    })
}

fn consumer<T>(
    client: &Client<T>,
    name: &'static str,
    keys: Vec<String>,
    consumer_tx: std_mpsc::Sender<Payload>,
) -> impl Future<Item = (), Error = io::Error> + Send + 'static
where
    T: AsyncRead + AsyncWrite,
    T: Send + Sync,
    T: 'static,
{
    let keys = stream::iter_ok::<_, ()>(keys);
    client.create_channel().and_then(move |channel| {
        let id = channel.id;
        println!("created channel with id: {}", id);

        let ch1 = channel.clone();
        let ch2 = channel.clone();

        channel
            .exchange_declare(
                EXCHANGE,
                EXCHANGE_TYPE,
                ExchangeDeclareOptions::default(),
                FieldTable::new(),
            ).and_then(move |_| {
                channel
                    .queue_declare(name, QueueDeclareOptions::default(), FieldTable::new())
                    .and_then(move |queue| {
                        println!("channel {} declared queue {}", id, name);

                        keys.for_each(move |key| {
                            channel
                                .queue_bind(
                                    name,
                                    EXCHANGE,
                                    &key,
                                    QueueBindOptions::default(),
                                    FieldTable::new(),
                                ).map(|_| ())
                                .map_err(|_| ())
                        }).map_err(|_| Error::new(ErrorKind::Other, "queue_bind error!"))
                        .map(|_| queue)
                    }).and_then(move |queue| {
                        ch1.basic_consume(
                            &queue,
                            name,
                            BasicConsumeOptions::default(),
                            FieldTable::new(),
                        )
                    })
            }).and_then(|stream| {
                stream.for_each(move |message| {
                    let Delivery {
                        delivery_tag,
                        routing_key,
                        data,
                        ..
                    } = message;
                    let ret = consumer_tx.send((routing_key, data));
                    if ret.is_err() {
                        println!("amqp message send error {:?}", ret);
                    }
                    ch2.basic_ack(delivery_tag, false)
                })
            })
    })
}

pub fn start(
    amqp_url: &str,
    name: &'static str,
    keys: &[&str],
    tx: std_mpsc::Sender<Payload>,
    rx: std_mpsc::Receiver<Payload>,
) {
    let uri = AMQPUri::from_str(amqp_url)
        .unwrap_or_else(|err| panic!("Failed to parse AMQP Url {}: {}", amqp_url, err));
    let addr = SocketAddr::from_str(&format!("{}:{}", uri.authority.host, uri.authority.port))
        .unwrap_or_else(|err| {
            panic!(
                "Failed to parse socket addr from AMQP Url {}: {}",
                amqp_url, err
            )
        });

    let (publisher_tx, publisher_rx) = mpsc::channel(65535);

    // Transfer data between different channels for api compatibility
    let _ = ::std::thread::Builder::new()
        .name("transfer".to_string())
        .spawn(move || {
            loop {
                if let Ok(data) = rx.recv() {
                    let _ = publisher_tx.clone().try_send(data);
                } else {
                    break;
                }
            }
            ::std::process::exit(0);
        });

    let keys: Vec<&str> = Vec::from(keys);
    let keys: Vec<String> = keys.into_iter().map(|x| x.to_owned()).collect();

    thread::spawn(move || {
        Runtime::new()
            .unwrap()
            .block_on_all(amqp_connect(uri, addr, name, keys, publisher_rx, tx))
    });
}
