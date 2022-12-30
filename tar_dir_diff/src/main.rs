use std::collections::HashMap;
use std::env;
use std::io::prelude::*;
use std::ffi::OsString;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process;

use getopts::Options;
use similar::TextDiff;

fn print_usage(mut stream: impl std::io::Write, program: &str) {
	let brief = format!("tar_dir_diff

Usage:
  {} [OPTION].. ARCHIVE DIR

Options:

", program);
	stream.write_all(brief.as_bytes()).unwrap();
}

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn print_version() {
	println!("tar_dir_diff version {}", VERSION)
}

fn list_files_in_directory(list: &mut HashMap<PathBuf, ()>, root_dir_path: &Path, cur_active_path: &Path) {
	let path = root_dir_path.join(cur_active_path);
	let current_dir = match fs::read_dir(&path) {
		Err(err) => {
			eprintln!("could not open directory {}: {}", path.to_string_lossy(), err);
			process::exit(1);
		},
		Ok(dir) => dir,
	};

	for dirent in current_dir {
		let dirent = match dirent {
			Err(err) => {
				eprintln!("could not read directory {}: {}", path.to_string_lossy(), err);
				process::exit(1);
			},
			Ok(dirent) => dirent,
		};
		let sub_path = cur_active_path.join(dirent.file_name());
		if dirent.path().is_dir() {
			list_files_in_directory(list, root_dir_path, &sub_path);
			continue
		} else if dirent.path().is_file() {
			list.insert(PathBuf::from(sub_path), ());
			continue
		} else {
			eprintln!("unexpected non-regular file {}", dirent.path().display());
			process::exit(1);
		}
	}
}

fn main() -> std::io::Result<()> {
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

	let archive_path = OsString::from(matches.free.remove(0));
	let root_dir_path = OsString::from(matches.free.remove(0));

	if matches.free.len() > 0 {
		panic!("matches.free.len() {}", matches.free.len());
	}

	let file = match File::open(&archive_path) {
		Err(err) => {
			eprintln!("could not open tar file {}: {}", archive_path.to_string_lossy(), err);
			process::exit(1);
		},
		Ok(file) => file,
	};

	let root_dir_path = Path::new(&root_dir_path);

	let mut root_dir_files = HashMap::new();
	list_files_in_directory(&mut root_dir_files, &root_dir_path, Path::new(""));

	let mut only_in_archive = vec![];

	let mut archive_files = HashMap::new();
	let mut archive = tar::Archive::new(file);
	for entry in archive.entries().unwrap() {
		let mut entry = entry.unwrap();

		let path = entry.header().path().unwrap();
		if path.is_absolute() {
			panic!("archive path {} is absolute", path.display());
		}
		let path = PathBuf::from(path);
		archive_files.insert(path.clone(), ());
		if !root_dir_files.contains_key(&path) {
			only_in_archive.push(path.to_string_lossy().into_owned());
			continue;
		}

		let mut archive_contents = String::new();
		entry.read_to_string(&mut archive_contents).unwrap();

		let file_path = root_dir_path.join(&path);
		let directory_contents = match fs::read_to_string(&file_path) {
			Err(err) => {
				eprintln!("could not read file {}: {}", archive_path.to_string_lossy(), err);
				process::exit(1);
			},
			Ok(data) => data,
		};

		let text_diff = TextDiff::from_lines(&archive_contents, &directory_contents);
		let udiff = text_diff
			.unified_diff()
			.context_radius(10)
			.header("live", "repository")
			.to_string();
		if udiff != "" {
			println!("{}", udiff);
		}
	}

	for path in only_in_archive {
		println!("{} only exists in live", path);
	}

	for (path, _value) in root_dir_files {
		if !archive_files.contains_key(&path) {
			println!("{} only exists in repo", path.display());
		}
	}

	Ok(())
}
