#![cfg_attr(feature="warnings-as-errors", deny(warnings))]

use std::collections::HashMap;
use std::env;
use std::io::{self, prelude::*};
use std::ffi::OsString;
use std::fs::File;
use std::path::PathBuf;
use std::process;

use getopts::Options;
use similar::{TextDiff, ChangeTag};

fn print_usage(mut stream: impl std::io::Write, program: &str) {
	let brief = format!("tar_diff

Usage:
  {} [OPTION].. ARCHIVE_A ARCHIVE_B

Options:
  --aname             name used for ARCHIVE_A in output
  --bname             name used for ARCHIVE_B in output

", program);
	stream.write_all(brief.as_bytes()).unwrap();
}

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn print_version() {
	println!("tar_diff version {}", VERSION)
}

fn read_archive_contents(path: &OsString) -> HashMap<PathBuf, String> {
	let mut contents = HashMap::new();

	let file: Box<dyn Read>;
	if path == "-" {
		file = Box::new(io::stdin());
	} else {
		file = match File::open(path) {
			Err(err) => {
				eprintln!("could not open archive {}: {}", path.to_string_lossy(), err);
				process::exit(1);
			},
			Ok(file) => Box::new(file),
		};
	}

	let mut archive = tar::Archive::new(file);
	for entry in archive.entries().unwrap() {
		let mut entry = entry.unwrap();

		let path = entry.header().path().unwrap();
		if path.is_absolute() {
			panic!("archive path {} is absolute", path.display());
		}
		let path = PathBuf::from(path);

		let entry_type = entry.header().entry_type();
		if entry_type == tar::EntryType::Directory {
			continue;
		} else if entry_type != tar::EntryType::Regular {
			panic!("archive entry {} is not a directory or a regular file", path.display());
		}

		let mut file_data = String::new();
		entry.read_to_string(&mut file_data).unwrap();

		contents.insert(path, file_data);
	}

	contents
}

fn main() {
	let args: Vec<String> = env::args().collect();
	let program = args[0].clone();

	let mut opts = Options::new();
	opts.optflag("h", "help", "print this help menu");
	opts.optflag("v", "version", "print version and exit");
	opts.optopt("", "aname", "name used for ARCHIVE_A in output", "aname");
	opts.optopt("", "bname", "name used for ARCHIVE_B in output", "bname");

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

	let aname = match matches.opt_str("aname") {
		Some(aname) => aname,
		None => String::from("ARCHIVE_A"),
	};
	let bname = match matches.opt_str("bname") {
		Some(bname) => bname,
		None => String::from("ARCHIVE_B"),
	};

	if matches.free.len() != 2 {
		print_usage(std::io::stderr(), &program);
		process::exit(1);
	}

	let archive_a_path = OsString::from(matches.free.remove(0));
	let archive_b_path = OsString::from(matches.free.remove(0));

	if matches.free.len() > 0 {
		panic!("matches.free.len() {}", matches.free.len());
	}

	let archive_a_contents = read_archive_contents(&archive_a_path);

	let file = match File::open(&archive_b_path) {
		Err(err) => {
			eprintln!("could not open archive {}: {}", archive_b_path.to_string_lossy(), err);
			process::exit(1);
		},
		Ok(file) => file,
	};

	let mut only_in_b = vec![];

	let mut archive_b_files = HashMap::new();
	let mut archive = tar::Archive::new(file);
	for entry in archive.entries().unwrap() {
		let mut entry = entry.unwrap();

		let path = entry.header().path().unwrap();
		if path.is_absolute() {
			panic!("archive path {} is absolute", path.display());
		}
		let path = PathBuf::from(path);

		let entry_type = entry.header().entry_type();
		if entry_type == tar::EntryType::Directory {
			continue;
		} else if entry_type != tar::EntryType::Regular {
			panic!("archive entry {} is not a directory or a regular file", path.display());
		}

		let file_data_a = archive_a_contents.get(&path);
		if file_data_a.is_none() {
			only_in_b.push(path.to_string_lossy().into_owned());
			continue;
		}
		let file_data_a = file_data_a.unwrap().to_owned();

		archive_b_files.insert(path.clone(), ());

		let mut file_data_b = String::new();
		entry.read_to_string(&mut file_data_b).unwrap();

		let text_diff = TextDiff::from_lines(&file_data_a, &file_data_b);
		let equal = !text_diff.iter_all_changes().any(|x| x.tag() != ChangeTag::Equal);
		if equal {
			continue;
		}
		let a = format!("{}/{}", aname, path.display());
		let b = format!("{}/{}", bname, path.display());
		let udiff = text_diff
			.unified_diff()
			.context_radius(6)
			.header(&a, &b)
			.to_string();
		println!("{}", udiff);
	}

	let mut only_in_a = vec![];

	for (path, _value) in archive_a_contents {
		if !archive_b_files.contains_key(&path) {
			only_in_a.push(path.to_string_lossy().into_owned());
		}
	}

	only_in_a.sort();
	for path in only_in_a {
		println!("{} only exists in {}", path, aname);
	}

	only_in_b.sort();
	for path in only_in_b {
		println!("{} only exists in {}", path, bname);
	}
}
