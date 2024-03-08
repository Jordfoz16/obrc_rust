use std::fs::File;
use std::sync::Arc;
use std::time::Instant;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::collections::{BTreeMap, HashMap};
use rayon::result;
use tokio::sync::{watch, oneshot, mpsc};
use tokio::task;
use log::{error, info, warn, Level};


struct Results {
    min: f32,
    max: f32,
    sum: f32,
    count: u32
}

const BUFFER_SIZE_KB: usize = 8;
const CHUNK_SIZE_KB: usize = 256;
const THREAD_COUNT: usize = 2;

async fn read_file(path: &str, sender: watch::Sender<Vec<u8>>) {

    let file: File = File::open(path).expect("Error");
    let mut reader: BufReader<File> = BufReader::with_capacity(BUFFER_SIZE_KB * 1024, file);
    let mut buffer = [0; CHUNK_SIZE_KB * 1024];

    loop {
        let bytes_read = reader.read(&mut buffer).expect("Error reading chunk");

        // End of File
        if bytes_read == 0 {
            break;
        }

        let last_newline = buffer[..bytes_read].into_iter().rposition(|&x| x == 10);

        match last_newline {
            Some(index) => {

                // Splits the chunk at the nearest \n character to ensure data isn't missed
                let (chunk, rest) = buffer[..bytes_read].split_at(index + 1);
                
                //Move the reader back to the start of the last \n so that it is included in the next chunk
                let reader_offset:i64 = 0 - rest.len() as i64;
                reader.seek(SeekFrom::Current(reader_offset)).unwrap();

                match sender.send(chunk.to_vec()){
                    Ok(_) => info!("Chunk Size {} Sent", chunk.len()),
                    Err(e) => log::error!("{}", e)
                };
            },
            None => {
                
                match sender.send(buffer[..bytes_read].to_vec()){
                    Ok(_) => info!("Chunk Size {} Sent", buffer[..bytes_read].len()),
                    Err(e) => log::error!("{}", e)
                };
            }
        }
    }

    drop(sender);
}

async fn process_chunk(mut receiver: watch::Receiver<Vec<u8>>, sender: mpsc::Sender<HashMap<String, Results>>, id: usize) {
    
    while receiver.changed().await.is_ok() {
        let chunk = receiver.borrow_and_update().to_owned();
        let sender_cell = sender.clone();
        
        let (send, recv) = oneshot::channel();
        rayon::spawn( move || {
            
            info!("Thread {}: Processing chunk with size {}", id, chunk.len());
            
            let lines = chunk.lines();
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
            let _ = send.send(city_map);
        });
        let test: HashMap<String, Results> = recv.await.expect("error");
        sender_cell.send(test).await.expect("Error sending");
        drop(sender_cell);
    } 
    
    drop(sender);
}

async fn combined_results(mut receiver: mpsc::Receiver<HashMap<String, Results>>){

    let mut final_results: HashMap<String, Results> = HashMap::new();

    while let Some(result) = receiver.recv().await {
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

    warn!("finished getting data");
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

// async fn print_u8_array(mut receiver: mpsc::Receiver<Vec<u8>>){
//     while let Some(value) = receiver.recv().await {
//         if let Ok(string) = std::str::from_utf8(&value) {
//             print!("{}", string);
//         } else {
//             println!("Invalid UTF-8 data");
//         }
//     }
// }

#[tokio::main]
async fn main(){
    let start_time = Instant::now();
    let path = "data/measurements_1b.txt";
    simple_logger::init_with_level(Level::Error).unwrap();

    let file_sender = watch::channel(Vec::new());
    let (result_sender, mut result_receiver) = mpsc::channel(100);

    let processing_receivers: Vec<_> = (0..THREAD_COUNT)
        .map(|id| {
            let processing_receiver = file_sender.1.clone();
            task::spawn(process_chunk(processing_receiver, result_sender.clone(), id))
        })
        .collect();

    let read_task = task::spawn(read_file(path, file_sender.0));
    let result_combined_task = task::spawn(combined_results(result_receiver));
    
    drop(result_sender);

    let _ = tokio::join!(read_task, result_combined_task, async {
        for handle in processing_receivers {
            handle.await.expect("Thread join error");
        }
    });

    

    let end_time = Instant::now();
    let elapsed_time = end_time.duration_since(start_time);
    error!("Elapsed time: {:?}", elapsed_time);

}