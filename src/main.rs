use std::fs::*;
use std::time::Instant;
use std::io::{BufRead, BufReader, Lines, Read, Seek, SeekFrom};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::{task, time};
use std::io::Result;
use log::*;


struct Results {
    min: f32,
    max: f32,
    sum: f32,
    count: u32
}

const BUFFER_SIZE_KB: usize = 8;
const CHUNK_SIZE_KB: usize = 256;

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

async fn process_chunk(mut receiver: watch::Receiver<Vec<u8>>) {
    while receiver.changed().await.is_ok() {
        let chunk = receiver.borrow_and_update();
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

        let sorted_results: BTreeMap<String, Results> = city_map.into_iter().collect();
    }    
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

async fn print_u8_array(mut receiver: mpsc::Receiver<Vec<u8>>){
    while let Some(value) = receiver.recv().await {
        if let Ok(string) = std::str::from_utf8(&value) {
            print!("{}", string);
        } else {
            println!("Invalid UTF-8 data");
        }
    }
}

#[tokio::main]
async fn main(){
    let start_time = Instant::now();
    let path = "data/measurements_10m.txt";
    simple_logger::init_with_level(Level::Debug).unwrap();

    let file_sender = watch::channel(Vec::new());

    let processing_receiver1 = file_sender.1.clone();
    let processing_receiver2 = file_sender.1.clone();

    let read_task = task::spawn(read_file(path, file_sender.0));
    let process_task1 = task::spawn(process_chunk(processing_receiver1));
    let process_task2 = task::spawn(process_chunk(processing_receiver2));

    let _ = tokio::try_join!(read_task, process_task1, process_task2).expect("error threads");

    let end_time = Instant::now();
    let elapsed_time = end_time.duration_since(start_time);
    warn!("Elapsed time: {:?}", elapsed_time);

}