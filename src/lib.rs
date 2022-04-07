use noodles_bed as bed;
use std::{env, error::Error, fs, path::Path, sync::Arc};

pub struct Args {
    pub input_file: String,
    pub output_file: String,
}

impl Args {
    pub fn new(mut args: env::Args) -> Result<Args, &'static str> {
        args.next();

        let input_file = match args.next() {
            Some(arg) => arg,
            None => return Err("Didn't get a file name"),
        };

        let output_file = match args.next() {
            Some(arg) => arg,
            None => return Err("Didn't get a file name"),
        };

        Ok(Args {
            input_file,
            output_file,
        })
    }
}

pub fn run(args: Args) -> Result<(), Box<dyn Error>> {
    load_file_convert_to_BedRecord_write_parquet(&args.input_file, &args.output_file);

    Ok(())
}

use parquet::{
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    record::RecordWriter,
    schema::parser::parse_message_type,
};
use parquet_derive::ParquetRecordWriter;

#[derive(ParquetRecordWriter, Debug, PartialEq, Eq)]
struct BedRecord {
    reference_sequence_name: String,
    // check if usize is enough
    start_position: usize,
    end_position: usize,
}

impl BedRecord {
    pub fn new(
        reference_sequence_name: String,
        start_position: usize,
        end_position: usize,
    ) -> Self {
        BedRecord {
            reference_sequence_name,
            start_position,
            end_position,
        }
    }
}

fn load_file_convert_to_BedRecord_write_parquet(input_file: &str, output_file: &str) {
    let data = fs::read(input_file).unwrap();

    let mut reader = bed::Reader::new(&data[..]);

    // how to handle errors?
    let bed_records_vec: Vec<BedRecord> = reader
        .records::<3>()
        .filter_map(|record| record.ok())
        .map(|record| convert_noodles_record_to_BedRecord(record))
        .collect();

    write_a_records_in_parquet(bed_records_vec, output_file);
}

fn convert_noodles_record_to_BedRecord(record: bed::Record<3>) -> BedRecord {
    BedRecord::new(
        record.reference_sequence_name().to_string(),
        usize::from(record.start_position()),
        usize::from(record.end_position()),
    )
}

fn write_a_records_in_parquet(bed_records_vec: Vec<BedRecord>, output_file: &str) {
    let message_type = "
  message schema {
      REQUIRED BYTE_ARRAY reference_sequence_name (UTF8);
      REQUIRED INT64 start_position;
      REQUIRED INT64 end_position;
}
";

    let path = Path::new(output_file);

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap();

    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group = writer.next_row_group().unwrap();

    (&bed_records_vec[..]).write_to_row_group(&mut row_group);

    writer.close_row_group(row_group).unwrap();
    writer.close().unwrap();
}

#[cfg(test)]
mod tests {
    use noodles_core::Position;

    use super::*;

    #[test]
    fn converting_to_our_representation() {
        let record = bed::Record::<3>::builder()
            .set_reference_sequence_name("sq0")
            .set_start_position(Position::try_from(8).unwrap())
            .set_end_position(Position::try_from(13).unwrap())
            .build()
            .unwrap();

        let expected = BedRecord::new("sq0".to_string(), 8, 13);

        let result = convert_noodles_record_to_BedRecord(record);

        assert_eq!(expected, result);
    }
}
