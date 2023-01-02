use std::env;
use std::ffi::OsString;
use std::fs::{self, File};
use std::path::Path;
use std::process;

use getopts::Options;

fn print_usage(mut stream: impl std::io::Write, program: &str) {
	let brief = format!("create_expected_archive

Usage:
  {} INPUT_DIR OUTPUT_FILE
", program);
	stream.write_all(brief.as_bytes()).unwrap();
}

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn print_version() {
	println!("create_expected_archive version {}", VERSION)
}

fn create_expected_archive(writer: &mut File, expected_dir_path: &Path) {
	let metadata = writer.metadata().unwrap();
	let mut archive = tar::Builder::new(writer);
	create_expected_archive_from_directory(&mut archive, &metadata, expected_dir_path, Path::new(""));
}

fn create_expected_archive_from_directory(archive: &mut tar::Builder<&mut File>, metadata: &fs::Metadata, directory: &Path, archive_path: &Path) {
	let dirfh = fs::read_dir(directory).unwrap();
	for dirent in dirfh {
		let dirent = dirent.unwrap();

		let file_type = dirent.file_type().unwrap();
		if file_type.is_dir() {
			create_expected_archive_from_directory(archive, metadata, &dirent.path(), &archive_path.join(dirent.file_name()));
		} else if !file_type.is_file() {
			panic!("unexpected file type {:?} for file {}", file_type, dirent.path().display());
		}

		let contents = "QWR :))".to_string();

		let mut header = tar::Header::new_gnu();
		header.set_metadata(metadata);
		header.set_size(contents.len() as u64);
		header.set_cksum();

		archive.append_data(&mut header, &archive_path.join(dirent.file_name()), contents.as_bytes()).unwrap();
	}
}

fn main() {
	let args: Vec<String> = env::args().collect();
	let program = args[0].clone();

	let mut opts = Options::new();
	opts.optflag("h", "help", "print this help menu");
	opts.optflag("v", "version", "print version and exit");

	let mut matches = match opts.parse(&args[1..]) {
		Err(f) => {
			eprintln!("{}: {}", &program, f.to_string());
			process::exit(1);
		},
		Ok(m) => m,
	};
	if matches.opt_present("h") {
		print_usage(std::io::stdout(), &program);
		process::exit(0);
	}
	if matches.opt_present("v") {
		print_version();
		process::exit(0);
	}

	if matches.free.len() != 2 {
		print_usage(std::io::stderr(), &program);
		process::exit(1);
	}

	let expected_dir_path = OsString::from(matches.free.remove(0));
	let output_path = OsString::from(matches.free.remove(0));

	if matches.free.len() > 0 {
		panic!("matches.free.len() {}", matches.free.len());
	}

	let mut output_file = File::create(output_path).unwrap();
	create_expected_archive(&mut output_file, Path::new(&expected_dir_path));
	output_file.sync_all().unwrap();
}
