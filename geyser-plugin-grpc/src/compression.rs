use std::io::{Read, Write};

pub fn zstd_compress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 0).unwrap();
    encoder.write_all(data)?;
    encoder.finish()
}

pub fn zstd_decompress(data: &[u8], uncompressed: &mut Vec<u8>) -> Result<usize, std::io::Error> {
    let mut decoder = zstd::stream::read::Decoder::new(data).unwrap();
    decoder.read_to_end(uncompressed)
}

pub(crate) mod tests {
    #[test]
    fn test_zstd_compression() {
        let data = vec![100; 256]; //sample data, 256 bytes of val 100.
        println!("Uncompressed Data = {:?}", data);

        match zstd_compress(&data) {
            Ok(compressed) => {
                println!("Compressed Data = {:?}\n", compressed);

                let mut uncompressed: Vec<u8> = Vec::new();

                match zstd_decompress(&compressed, &mut uncompressed) {
                    Ok(_) => {
                        println!("Uncompressed Data = {:?}\n", uncompressed);
                    }
                    Err(e) => {
                        println!("Error = {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("Error compressing Data {:?}", e);
            }
        }
    }
}
