use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Lines, BufRead, BufReader, Read, Seek, SeekFrom};
use std::ptr::read;
use std::time::Instant;
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

const BUFFER_CAPACITY: usize =  8000;

const READ_BUFFER_SIZE_KB: usize = 16;
const CHUNK_SIZE_KB: usize = 1024;

fn read_file(path: &str) -> HashMap<String, Results>{

    let mut results: HashMap<String, Results> = HashMap::new();

    let file: File = File::open(path).expect("Error");
    let mut reader: BufReader<File> = BufReader::with_capacity(READ_BUFFER_SIZE_KB * 1024, file);
    let mut buffer = [0; CHUNK_SIZE_KB * 1024];
    let mut count = 0;

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

                // let _ = tx.send_blocking(Chunk { id: count, chunk: chunk.to_vec() });
                // info!("Queue Length {}", tx.len());
                process_chunk(Chunk { id: count, chunk: chunk.to_vec() }, &mut results);
            },
            None => {
                // let _ = tx.send_blocking(Chunk { id: count, chunk: buffer[..bytes_read].to_vec() });
                // info!("Queue Length {}", tx.len());
                process_chunk(Chunk { id: count, chunk: buffer[..bytes_read].to_vec() }, &mut results);
            }
        }

        info!("Chunk Created: {}", count);
        count += 1;
    }

    return results;
}

fn process_chunk(rx: Chunk,  result_tx: &mut HashMap<String, Results>){
    
    let lines = rx.chunk.lines();
    let mut city_map = result_tx;
    
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

fn main() {
    
    let start_time = Instant::now();

    //let lines: Lines<BufReader<File>> = read_file();
    let path = "data/measurements_10m.txt";
    let results = read_file(path);

    //let results: HashMap<String, Results> = calculate(lines);
    let sorted_results: BTreeMap<String, Results> = results.into_iter().collect();
    
    print_results(sorted_results);

    let end_time = Instant::now();
    let elapsed_time = end_time.duration_since(start_time);
    println!("Elapsed time: {:?}", elapsed_time);
}