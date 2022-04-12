# bed_to_parquet_conversion

This is demo, to try it out run:

`cargo run samples/well_formed_sample.bed output.parquet`

## Achievables

- [x] Binary that converts a `.bed` with the 3 basic columns {reference_sequence_name, start_position, end_position} to a `.parquet`.

Integration Tests:
- [ ] Acceptance basic test
    (troublesome to replicate because of specific parquet.rs objects)

- [ ] Graciously reject badly formed .bed file
- [ ] Graciously reject inexistant .bed file

Error messages
- [ ] Graciously reject badly formed .bed file
- [ ] Graciously reject inexistant .bed file
- [ ] Make acceptable catch all solution and reporting

