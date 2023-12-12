use std::env;
use std::ffi::OsString;
use std::io::Read;
use std::fs::{self, File};
use std::path::Path;
use std::process;

use getopts::Options;

fn print_usage(mut stream: impl std::io::Write, program: &str) {
	let brief = format!("create_expected_archive

Usage:
  {} POSTGRES_VERSION INPUT_DIR OUTPUT_FILE
", program);
	stream.write_all(brief.as_bytes()).unwrap();
}

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn print_version() {
	println!("create_expected_archive version {}", VERSION)
}

fn parse_postgres_major_version(server_version_num: String) -> i32 {
	let i = server_version_num.parse::<i32>().unwrap();
	if i < 110000 || i > 170000 {
		panic!("invalid postgres server_version_num {}", i);
	}
	return i / 10000;
}

fn create_expected_archive(postgres_version: i32, writer: &mut File, expected_dir_path: &Path) {
	let mut archive = tar::Builder::new(writer);
	create_expected_archive_from_directory(postgres_version, &mut archive, expected_dir_path, Path::new(""));
}

fn create_expected_archive_from_directory(postgres_version: i32, archive: &mut tar::Builder<&mut File>, directory: &Path, archive_path: &Path) {
	let dirfh = fs::read_dir(directory).unwrap();
	'outer: for dirent in dirfh {
		let dirent = dirent.unwrap();

		let file_type = dirent.file_type().unwrap();
		if file_type.is_dir() {
			create_expected_archive_from_directory(postgres_version, archive, &dirent.path(), &archive_path.join(dirent.file_name()));
			continue 'outer;
		} else if !file_type.is_file() {
			panic!("unexpected file type {:?} for file {}", file_type, dirent.path().display());
		}

		if dirent.file_name().to_string_lossy().starts_with(".") {
			continue 'outer;
		}

		let mut fh = File::open(dirent.path()).unwrap();
		let metadata = fh.metadata().unwrap();

		let mut contents = "".to_string();
		match fh.read_to_string(&mut contents) {
			Err(err) => panic!("could not read file {}: {}", dirent.path().display(), err),
			Ok(_len) => {},
		};

		let mut directives = vec![];
		while contents.starts_with("-- !!") {
			let end = contents.find("\n").unwrap();
			let mut directive: String = contents.drain(..end + 1).collect();
			directive.truncate(directive.trim_end().len());
			directives.push(directive);
		}

		let mut file_archive_path = archive_path.join(dirent.file_name());

		for directive in directives {
			if let Some(location) = directive.strip_prefix("-- !! LOC ") {
				file_archive_path = Path::new(location).to_path_buf();
			} else if let Some(version_expression) = directive.strip_prefix("-- !! VER ") {
				// TODO, obviously
				if version_expression == "< 12" {
					if !(postgres_version < 12) {
						continue 'outer;
					}
				} else if version_expression == ">= 12" {
					if !(postgres_version >= 12) {
						continue 'outer;
					}
				} else {
					panic!("unexpected version expression {}", version_expression);
				}
			} else {
				panic!("unknown directive {}", directive);
			}
		}

		let mut header = tar::Header::new_gnu();
		header.set_metadata(&metadata);
		header.set_size(contents.len() as u64);
		header.set_cksum();

		archive.append_data(&mut header, &file_archive_path, contents.as_bytes()).unwrap();
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

	if matches.free.len() != 3 {
		print_usage(std::io::stderr(), &program);
		process::exit(1);
	}

	let postgres_version = parse_postgres_major_version(matches.free.remove(0));
	let expected_dir_path = OsString::from(matches.free.remove(0));
	let output_path = OsString::from(matches.free.remove(0));

	if matches.free.len() > 0 {
		panic!("matches.free.len() {}", matches.free.len());
	}

	let mut output_file = File::create(output_path).unwrap();
	create_expected_archive(postgres_version, &mut output_file, Path::new(&expected_dir_path));
	output_file.sync_all().unwrap();
}
