#![cfg_attr(feature="warnings-as-errors", deny(warnings))]

use std::ffi::OsString;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::process;

use getopts::Options;

mod auxiliary_data;
mod custom_dump_reader;
mod postgres_configuration;
mod pg_dump_subprocess;
mod output;

use custom_dump_reader::SplitDumpDirectory;
use output::*;

fn print_usage(mut stream: impl std::io::Write, program: &str) {
	let brief = format!("pg_split_dump takes a schema-only dump into a directory format

Usage:
  {} [OPTION].. CONNINFO OUTPUT

Options:
  --pg-dump-binary=PG_DUMP_PATH
                      use the pg_dump binary in PG_DUMP_PATH
  --format=d|t
                      output file format: directory or tar archive; the default
                      is a directory unless OUTPUT ends in \".tar\"

", program);
	stream.write_all(brief.as_bytes()).unwrap();
}

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn print_version() {
	println!("pg_split_dump version {}", VERSION)
}

fn write_split_directory_contents(dir_path: &Path, contents: &SplitDumpDirectory, create: bool) {
	if create {
		if let Err(err) = fs::create_dir(dir_path) {
			eprintln!("could not create output subdirectory {}: {}", dir_path.display(), err);
			process::exit(1);
		}
	}

	for (filename, file_contents) in &contents.files {
		let path = dir_path.join(filename);

		let mut file = match File::create(&path) {
			Err(err) => {
				eprintln!("could not create output file {}: {}", path.display(), err);
				process::exit(1);
			},
			Ok(file) => file,
		};
		for line in file_contents {
			if let Err(err) = write!(file, "{}\n", line) {
				eprintln!("could not write to output file {}: {}", path.display(), err);
				process::exit(1);
			}
		}
		if let Err(err) = file.sync_all() {
			eprintln!("could not write to output file {}: {}", path.display(), err);
			process::exit(1);
		}
	}

	for (subdir, subdir_contents) in &contents.dirs {
		let subdir_path = dir_path.join(subdir);
		write_split_directory_contents(&subdir_path, subdir_contents, true);
	}
}

fn main() -> std::io::Result<()> {
	let args: Vec<String> = env::args().collect();
	let program = args[0].clone();

	let mut opts = Options::new();
	opts.optflag("h", "help", "print this help menu");
	opts.optflag("v", "version", "print version and exit");
	opts.optopt("", "pg-dump-binary", "use the pg_dump binary in PG_DUMP_PATH", "PG_DUMP_PATH");
	opts.optopt("", "format", "output format", "FORMAT");

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
	let pg_dump_binary = match matches.opt_str("pg-dump-binary") {
		Some(pg_dump_binary) => pg_dump_binary,
		None => panic!("pg-dump-binary is currently required"),
	};
	let pg_dump_binary = OsString::from(pg_dump_binary);

	let output_format = match matches.opt_str("format") {
		Some(fmt) => {
			let output_format = OutputFormat::from_string(&fmt);
			if output_format.is_none() {
				eprintln!("invalid output format {}", fmt);
				process::exit(1);
			}
			output_format
		},
		None => None,
	};

	if matches.free.len() < 2 {
		print_usage(std::io::stderr(), &program);
		process::exit(1);
	}

	let conninfo = matches.free.remove(0);
	let output_path = OsString::from(matches.free.remove(0));
	if matches.free.len() > 0 {
		print_usage(std::io::stderr(), &program);
		process::exit(1);
	}

	let output_format = match output_format {
		Some(fmt) => fmt,
		None => if output_path.to_string_lossy().ends_with(".tar") {
			OutputFormat::TarArchive
		} else {
			OutputFormat::Directory
		},
	};

	let output_path = Path::new(&output_path);
	if output_path.exists() {
		eprintln!("output {} already exists", output_path.display());
		process::exit(1);
	}

	let pg_config = postgres_configuration::create(&conninfo);

	let pg_conn = pg_config.connect(postgres::NoTls);
	if let Err(e) = pg_conn {
		eprintln!("could not connect to postgres: {}", e);
		process::exit(1);
	}
	let mut pg_conn = pg_conn.unwrap();

	let res = pg_conn.execute("SET default_transaction_read_only TO TRUE", &[]);
	if let Err(e) = res {
		eprintln!("could not set default_transaction_read_only: {}", e);
		process::exit(1);
	}

	let mut txn = match pg_conn.transaction() {
		Err(e) => {
			eprintln!("could not begin a database transaction: {}", e);
			process::exit(1);
		},
		Ok(txn) => txn,
	};

	let row = match txn.query_one("SELECT pg_export_snapshot()", &[]) {
		Err(e) => {
			eprintln!("could not export a database snapshot: {}", e);
			process::exit(1);
		},
		Ok(row) => row,
	};
	let snapshot_id: String = row.get(0);

	let pg_dump = match pg_dump_subprocess::PgDumpSubprocess::new(&pg_dump_binary, &conninfo, &snapshot_id) {
		Err(_err) => {
			//eprintln!("could not start pg_dump subprocess: {}", err);
			process::exit(1);
		},
		Ok(pg_dump) => pg_dump,
	};

	let aux_data = match auxiliary_data::query(&mut txn) {
		Err(err) => {
			eprintln!("{}", err);
			process::exit(1);
		},
		Ok(aux_data) => aux_data,
	};

	let dump = match custom_dump_reader::read_dump(pg_dump, &aux_data) {
		Err(err) => {
			panic!("{:?}", err);
		},
		Ok(dump) => dump,
	};

	if let Err(err) = txn.commit() {
		eprintln!("could not commit our database transaction: {}", err);
		process::exit(1);
	}

	if output_format == OutputFormat::Directory {
		if let Err(err) = fs::create_dir(&output_path) {
			eprintln!("could not create output directory: {}", err);
			process::exit(1);
		}

		write_split_directory_contents(&output_path, &dump.split_root, false);
	} else if output_format == OutputFormat::TarArchive {
		let mut writer = match TarOutputWriter::new(output_path) {
			Err(err) => {
				eprintln!("could not start writing to output archive: {:?}", err);
				process::exit(1);
			},
			Ok(writer) => writer,
		};

		if let Err(err) = writer.write_from_split_dump(&dump.split_root) {
			eprintln!("could not start writing to output archive: {:?}", err);
			process::exit(1);
		}

		writer.sync();
	} else {
		panic!("{:?}", output_format);
	}

	Ok(())
}
