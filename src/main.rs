use std::fs::*;
use std::time::Instant;
use std::io::{BufRead, BufReader, Lines, Read, Seek, SeekFrom};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::mpsc;
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

async fn read_file(path: &str, sender: mpsc::Sender<Vec<u8>>) {

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

                match sender.send(chunk.to_vec()).await{
                    Ok(_) => info!("Chunk Size {} Sent", chunk.len()),
                    Err(e) => log::error!("{}", e)
                };
            },
            None => {
                
                match sender.send(buffer[..bytes_read].to_vec()).await{
                    Ok(_) => info!("Chunk Size {} Sent", buffer[..bytes_read].len()),
                    Err(e) => log::error!("{}", e)
                };
            }
        }

    }
}

async fn process_chunk(mut receiver: mpsc::Receiver<Vec<u8>>) {
    while let Some(chunk) = receiver.recv().await {
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
    simple_logger::init_with_level(Level::Info).unwrap();

    let (sender, mut receiver) = mpsc::channel(100);

    let read_task = task::spawn(async move {
        read_file(path, sender).await;  
    });

    let process_task = task::spawn(async move {
        process_chunk(receiver).await;  
    });

    tokio::join!(read_task, process_task);

    let end_time = Instant::now();
    let elapsed_time = end_time.duration_since(start_time);
    warn!("Elapsed time: {:?}", elapsed_time);

}

// fn read_file2(path: &str){
//     let file = match File::open(path) {
//         Ok(f) => f,
//         Err(e) => panic!("{}", e)
//     };

//     let reader = BufReader::with_capacity(BUFFER_SIZE_KB,file);

//     let lines: Lines<BufReader<File>> = reader.lines();

//     for line in lines{
//         println!("{}", line.unwrap());
//     }
// }

// fn print_u8_array(bytes: &[u8]){
//     if let Ok(string) = std::str::from_utf8(&bytes) {
//         print!("{}", string);
//     } else {
//         println!("Invalid UTF-8 data");
//     }
// }

// let (sender, mut receiver) = mpsc::channel(1024);

//     let read_task = task::spawn(async move {
//         read_file("data/measurements_4.txt", sender).await;  
//     });

//     let (sender1, receiver1) = mpsc::channel(1024);
//     // let (sender2, receiver2) = mpsc::channel(100);

//     let calculate_task1 = task::spawn(async move {
//         process_chunk(receiver1).await;
//     });

//     // let calculate_task2 = task::spawn(async move {
//     //     process_chunk(receiver2).await;
//     // });

//     while let Some(chunk) = receiver.recv().await {
//         if let Err(_) = sender1.send(chunk.clone()).await {
//             break;
//         }
//         // if let Err(_) = sender2.send(chunk).await {
//         //     break;
//         // }
//     }

//     //tokio::join!(read_task, calculate_task1);
