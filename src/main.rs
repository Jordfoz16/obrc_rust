use std::fs::*;
use std::time::Instant;
use std::io::{BufRead, BufReader, Lines, Read, Seek, SeekFrom};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::mpsc;
use tokio::task;

struct Results {
    min: f32,
    max: f32,
    sum: f32,
    count: u32
}

const BUFFER_SIZE_KB: usize = 8;
const CHUNK_SIZE_KB: usize = 256;

fn read_file(path: &str){

    let file: File = File::open(path).expect("Error reading file");
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
                //sender.send(chunk.to_vec()).await?;
                
                //Move the reader back to the start of the last \n so that it is included in the next chunk
                let reader_offset:i64 = 0 - rest.len() as i64;
                reader.seek(SeekFrom::Current(reader_offset)).unwrap();

                process_chunk(&chunk);
            },
            None => {
                //sender.send(buffer[..bytes_read].to_vec()).await?;
                process_chunk(&buffer[..bytes_read]);
            }
        }

    }
}

fn process_chunk(chunk: &[u8]) -> BTreeMap<String, Results>{
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

    return sorted_results;
}

fn read_file2(path: &str){
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => panic!("{}", e)
    };

    let reader = BufReader::with_capacity(BUFFER_SIZE_KB,file);

    let lines: Lines<BufReader<File>> = reader.lines();

    for line in lines{
        println!("{}", line.unwrap());
    }
}

fn print_u8_array(bytes: &[u8]){
    if let Ok(string) = std::str::from_utf8(&bytes) {
        print!("{}", string);
    } else {
        println!("Invalid UTF-8 data");
    }
}
fn main(){
    let start_time = Instant::now();

    read_file("data/measurements_1b.txt");

    let end_time = Instant::now();
    let elapsed_time = end_time.duration_since(start_time);
    println!("Elapsed time: {:?}", elapsed_time);
}