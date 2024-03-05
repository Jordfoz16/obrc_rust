use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::File;
use std::io::{Lines, BufRead, BufReader};
use std::time::Instant;


struct Measurement {
    city: String,
    temp: f32
}

struct Results {
    min: f32,
    max: f32,
    sum: f32,
    count: u32
}

const BUFFER_CAPACITY: usize = 64000 * 10;

fn read_file() -> Lines<BufReader<File>>{
    let file = match File::open("data/measurements_10m.txt") {
        Ok(f) => f,
        Err(e) => panic!("{}", e)
    };

    let reader = BufReader::with_capacity(BUFFER_CAPACITY,file);

    let lines: Lines<BufReader<File>> = reader.lines();

    return lines;
}


fn parse_line(line: &str) -> Measurement {
    //Saint Petersburg;7.5
    let mut split_line = line.split(";");

    let city = split_line.next().unwrap().to_string();
    let temp: f32 = split_line.next().unwrap().parse().unwrap();

    let measurement = Measurement{city: city, temp: temp};

    return measurement;
}


fn calculate_results(measurements: Vec<Measurement>) -> HashMap<String, Results>{
    
    let mut city_map: HashMap<String, Results> = HashMap::new();

    for m in measurements {
        
        let current_measurement = city_map.entry(m.city.clone()).or_insert(Results {
            min: f32::INFINITY,
            max: f32::NEG_INFINITY,
            sum: 0.0,
            count: 0,
        });
        
        if m.temp < current_measurement.min {
            current_measurement.min = m.temp;
        }

        if m.temp > current_measurement.max {
            current_measurement.max = m.temp;
        }

        current_measurement.sum += m.temp;
        current_measurement.count += 1;
        
    }

    return city_map;
    
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

    fs::write("result.txt", &result).expect("Error writing to file");
    //println!("{}", result);

}

fn main() {
    
    let start_time = Instant::now();

    let lines = read_file();

    let mut measurements: Vec<Measurement> = vec![];

    for line in lines {
        let t = line.unwrap().clone();
        measurements.push(parse_line(&t));
    }

    let results: HashMap<String, Results> = calculate_results(measurements);
    let sorted_results: BTreeMap<String, Results> = results.into_iter().collect();

    print_results(sorted_results);

    let end_time = Instant::now();
    let elapsed_time = end_time.duration_since(start_time);

    println!("Elapsed time: {:?}", elapsed_time);
}