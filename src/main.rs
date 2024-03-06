use std::fs::*;
use std::time::Instant;
use std::io::{BufRead, BufReader, Lines, Read, Seek, SeekFrom};

const BUFFER_SIZE_KB: usize = 6400000;

fn read_file(path: &str){

    let file: File = File::open(path).expect("Error reading file");
    let mut reader: BufReader<File> = BufReader::new(file);
    let mut buffer = [0; BUFFER_SIZE_KB];

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

                print_u8_array(&chunk);
            },
            None => {
                //sender.send(buffer[..bytes_read].to_vec()).await?;
                print_u8_array(&buffer[..bytes_read]);
            }
        }

    }
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

    read_file2("data/measurements_1m.txt");

    let end_time = Instant::now();
    let elapsed_time = end_time.duration_since(start_time);
    println!("Elapsed time: {:?}", elapsed_time);
}