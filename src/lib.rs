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
    let data = fs::read(&args.input_file)?;

    let mut reader = bed::Reader::new(&data[..]);

    let bed_records_vec = reader
        .records::<3>()
        .filter_map(|record| record.ok())
        .map(convert_noodles_record_to_bedrecord)
        .collect();

    write_a_records_in_parquet(bed_records_vec, &args.output_file)?;

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

// u64 value chosen according to BED official definition
// https://samtools.github.io/hts-specs/BEDv1.pdf
#[derive(ParquetRecordWriter, Debug, PartialEq, Eq)]
struct BedRecord {
    reference_sequence_name: String,
    start_position: u64,
    end_position: u64,
}

impl BedRecord {
    pub fn new(reference_sequence_name: String, start_position: u64, end_position: u64) -> Self {
        BedRecord {
            reference_sequence_name,
            start_position,
            end_position,
        }
    }
}

fn convert_noodles_record_to_bedrecord(record: bed::Record<3>) -> BedRecord {
    BedRecord::new(
        record.reference_sequence_name().to_string(),
        usize::from(record.start_position()) as u64,
        usize::from(record.end_position()) as u64,
    )
}

fn write_a_records_in_parquet(
    bed_records_vec: Vec<BedRecord>,
    output_file: &str,
) -> Result<(), Box<dyn Error>> {
    // There is no reference to Unsigned integers on the schema parser
    // It seems like if we are only using this for storage reasons,
    // this would have the same behaviour as a unsigned definition
    let message_type = "
  message schema {
      REQUIRED BYTE_ARRAY reference_sequence_name (UTF8);
      REQUIRED INT64 start_position;
      REQUIRED INT64 end_position;
}
";

    let path = Path::new(output_file);

    let schema = Arc::new(parse_message_type(message_type)?);
    let props = Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path)?;

    let mut writer = SerializedFileWriter::new(file, schema, props)?;
    let mut row_group = writer.next_row_group()?;

    (&bed_records_vec[..]).write_to_row_group(&mut row_group)?;

    writer.close_row_group(row_group)?;
    writer.close()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use noodles_core::Position;

    use super::*;

    #[test]
    fn test_converting_noodles_to_our_representation() {
        let record = bed::Record::<3>::builder()
            .set_reference_sequence_name("sq0")
            .set_start_position(Position::try_from(8).unwrap())
            .set_end_position(Position::try_from(13).unwrap())
            .build()
            .unwrap();

        let expected = BedRecord::new("sq0".to_string(), 8, 13);

        let result = convert_noodles_record_to_bedrecord(record);

        assert_eq!(expected, result);
    }
}
