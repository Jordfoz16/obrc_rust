use std::fs::File;
use std::time::Instant;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::oneshot;
use tokio::task;
use log::{error, info, Level};
use ahash::AHashMap;


struct Results {
    min: f32,
    max: f32,
    sum: f32,
    count: u32
}
#[derive(Clone)]
struct Chunk {
    id: usize,
    chunk: Vec<u8>
}

const READ_BUFFER_SIZE_KB: usize = 16 * 1024;
const CHUNK_SIZE_KB: usize = 1024 * 1024;
const THREAD_COUNT: usize = 12;
const CHANNEL_BUFFER_SIZE: usize = 100;


async fn read_file(path: &str, tx: async_channel::Sender<Chunk>) {

    let file: File = File::open(path).expect("Error");
    let mut reader: BufReader<File> = BufReader::with_capacity(READ_BUFFER_SIZE_KB, file);
    let mut buffer = [0; CHUNK_SIZE_KB];
    let mut count = 0;

    tx.downgrade();

    loop {
        let bytes_read = reader.read(&mut buffer).expect("Error reading chunk");
        // End of File
        if bytes_read == 0 {
            break;
        }

        let mut split_index: Option<usize> = None;

        if *buffer.last().unwrap() == 0 as u8 {
            split_index = None;
        }else if *buffer.last().unwrap() == 10 as u8{
            split_index = None;
        }else {
            split_index = buffer[..bytes_read].into_iter().rposition(|&x| x == 10);
        }

        match split_index {
            Some(index) => {

                // Splits the chunk at the nearest \n character to ensure data isn't missed
                let (chunk, rest) = buffer[..bytes_read].split_at(index + 1);
                
                //Move the reader back to the start of the last \n so that it is included in the next chunk
                let reader_offset:i64 = 0 - rest.len() as i64;
                reader.seek(SeekFrom::Current(reader_offset)).unwrap();

                let _ = tx.send_blocking(Chunk { id: count, chunk: chunk.to_vec() });
                info!("Queue Length {}", tx.len());
            },
            None => {
                let _ = tx.send_blocking(Chunk { id: count, chunk: buffer[..bytes_read].to_vec() });
                info!("Queue Length {}", tx.len());
            }
        }

        info!("Chunk Created: {}", count);
        count += 1;
    }
}

async fn process_chunk(rx: async_channel::Receiver<Chunk>, result_tx: async_channel::Sender<AHashMap<String, Results>>, thread_id: usize) {
    
    result_tx.downgrade();

    while let Ok(msg) = rx.recv().await {

        let (completion_tx, completion_rx) = oneshot::channel();

        rayon::spawn( move || {
            
            info!("Thread {}: Processing chunk {}", thread_id, msg.id);
            
            let lines = msg.chunk.lines();
            let mut city_map: AHashMap<String, Results> = AHashMap::new();
            
            for line in lines {
                let line_string = line.unwrap();
                let mut split_line = line_string.split(";");

                let city = split_line.next().unwrap().to_string();
                let temp: f32 = split_line.next().unwrap().parse().unwrap();

                let current_measurement = city_map.entry(city.clone()).or_insert(Results {
                    min: f32::INFINITY,
                    max: f32::NEG_INFINITY,
                    sum: 0.0,
                    count: 0,
                });

                if temp < current_measurement.min {
                    current_measurement.min = temp;
                } else if temp > current_measurement.max {
                    current_measurement.max = temp;
                }

                current_measurement.sum += temp;
                current_measurement.count += 1;

            }

            let _ = completion_tx.send(city_map);
        });

        let result = completion_rx.await.unwrap();
        let _ = result_tx.send(result).await;
    }

    result_tx.close();
}

async fn combined_results(receiver: async_channel::Receiver<AHashMap<String, Results>>){

    let mut final_results: AHashMap<String, Results> = AHashMap::new();
    
    while let Ok(result) = receiver.recv().await {
        info!("Recieved {:?} results", result.len());
        for (k, v) in result{
            if final_results.contains_key(&k){
                let final_result = final_results.get_mut(&k).unwrap();
                
                if v.min < final_result.min {
                    final_result.min = v.min;
                }

                if v.max > final_result.max {
                    final_result.max = v.max;
                }

                final_result.count += v.count;
                final_result.sum += v.sum;
                
                
            }else{
                final_results.insert(k, v);
            }
            
        }
    }

    info!("Finished combining data");
    let sorted_results: BTreeMap<String, Results> = final_results.into_iter().collect();
    print_results(sorted_results);
}

fn print_results(results: BTreeMap<String, Results>){

    let mut result: String = String::new();

    result += "{";

    for (key, value) in results.iter() {
        result += format!("{}=", key.as_str()).as_str();
        result += format!("{:.1}/", value.min).as_str();
        
        let avg = value.sum / value.count as f32;

        result += format!("{:.1}/", avg).as_str();
        result += format!("{:.1}", value.max).as_str();
        
        result += ", ";
    }

    result.pop();
    result.pop();

    result += "}";

    println!("{}", result);

}

#[tokio::main]
async fn main(){
    let start_time = Instant::now();
    let path = "data/measurements_1b.txt";
    simple_logger::init_with_level(Level::Error).unwrap();
    
    let mem_estimate = (CHUNK_SIZE_KB * CHANNEL_BUFFER_SIZE) / 1024;
    error!("Starting: Reader Buffer Size: {} KB, Chunk Size: {} KB, Estimated RAM Usage: {} MB, Thread Count: {}", READ_BUFFER_SIZE_KB / 1024, CHUNK_SIZE_KB / 1024, mem_estimate / 1024, THREAD_COUNT);

    let (chunk_tx, chunk_rx) = async_channel::bounded(CHANNEL_BUFFER_SIZE);
    let (result_tx, result_rx) = async_channel::unbounded();

    let mut handles = vec![];

    for thread_id in 0..THREAD_COUNT {
        let chunk_rx_clone = chunk_rx.clone();
        let result_tx_clone = result_tx.clone();
        let handle = tokio::spawn(async move {
            process_chunk(chunk_rx_clone, result_tx_clone, thread_id).await;
        });
        handles.push(handle);
    }

    let read_task = task::spawn(read_file(path, chunk_tx));
    let combined_task = task::spawn(combined_results(result_rx));

    let _ = tokio::join!(read_task, combined_task, async {
        for handle in handles {
            handle.await.expect("Thread join error");
        }
    });

    let end_time = Instant::now();
    let elapsed_time = end_time.duration_since(start_time);
    error!("Elapsed time: {:?}", elapsed_time);

}