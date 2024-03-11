use std::fs::File;
use std::time::Instant;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::collections::{BTreeMap, HashMap};
use rayon::result;
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use log::{error, info, Level};


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

const BUFFER_SIZE_KB: usize = 8;
const CHUNK_SIZE_KB: usize = 256;
const THREAD_COUNT: usize = 2;

async fn read_file(path: &str, tx: async_channel::Sender<Chunk>) {

    let file: File = File::open(path).expect("Error");
    let mut reader: BufReader<File> = BufReader::with_capacity(BUFFER_SIZE_KB * 1024, file);
    let mut buffer = [0; CHUNK_SIZE_KB * 1024];
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

                //chunk.to_vec()
                let _ = tx.send(Chunk { id: count, chunk: chunk.to_vec() }).await;
                //info!("Queue Length {}", tx.len());
                
            },
            None => {
                
                //buffer[..bytes_read].to_vec()
                let _ = tx.send(Chunk { id: count, chunk: buffer[..bytes_read].to_vec() }).await;
                //info!("Queue Length {}", tx.len());
            }
        }

        //info!("Chunk Created: {}", count);
        count += 1;
    }
}

async fn process_chunk(rx: async_channel::Receiver<Chunk>, result_tx: async_channel::Sender<HashMap<String, Results>>, thread_id: usize) {
    
    result_tx.downgrade();

    while let Ok(msg) = rx.recv().await {
        //let msg = rx.recv().await.expect("error");
        info!("sender count {}, recieiver count {}", rx.sender_count(), rx.receiver_count());

        let (completion_tx, completion_rx) = oneshot::channel();

        rayon::spawn( move || {
            
            info!("Thread {}: Processing chunk {}", thread_id, msg.id);
            
            let lines = msg.chunk.lines();
            let mut city_map: HashMap<String, Results> = HashMap::new();
            
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
            }

            if temp > current_measurement.max {
                current_measurement.max = temp;
            }

            current_measurement.sum += temp;
            current_measurement.count += 1;

            }

            //let sorted_results: BTreeMap<String, Results> = city_map.into_iter().collect();
            let _ = completion_tx.send(city_map);
        });

        let result = completion_rx.await.unwrap();
        let _ = result_tx.send(result).await;
    }

    result_tx.close();
}

async fn combined_results(receiver: async_channel::Receiver<HashMap<String, Results>>){

    let mut final_results: HashMap<String, Results> = HashMap::new();
    
    while let Ok(result) = receiver.recv().await {
        info!("{} {}", receiver.sender_count(), receiver.receiver_count());
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

    //fs::write("result.txt", &result).expect("Error writing to file");
    println!("{}", result);

}

#[tokio::main]
async fn main(){
    let start_time = Instant::now();
    let path = "data/measurements_1m.txt";
    simple_logger::init_with_level(Level::Debug).unwrap();
    
    //let (file_tx, mut file_rx) = mpsc::channel(100);
    let (chunk_tx, chunk_rx) = async_channel::unbounded();
    let (result_tx, result_rx) = async_channel::unbounded();

    chunk_tx.downgrade();

    let mut handles = vec![];

    for thread_id in 0..THREAD_COUNT {
        let rx_clone = chunk_rx.clone();
        let results_clone = result_tx.clone();
        let handle = tokio::spawn(async move {
            process_chunk(rx_clone, results_clone, thread_id).await;
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