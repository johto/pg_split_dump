use std::ffi::OsString;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::process;

use getopts::Options;

mod auxiliary_data;
mod custom_dump_reader;
mod pg_dump_subprocess;

fn print_usage(mut stream: impl std::io::Write, program: &str) {
	let brief = format!("pg_split_dump takes a schema-only dump into a directory format

Usage:
  {} [OPTION].. CONNINFO OUTPUTDIR

Options:
  --pg-dump-binary=PG_DUMP_PATH
                      use the pg_dump binary in PG_DUMP_PATH

", program);
	stream.write_all(brief.as_bytes()).unwrap();
}

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn print_version() {
	println!("pg_split_dump version {}", VERSION)
}

fn main() -> std::io::Result<()> {
	let args: Vec<String> = env::args().collect();
	let program = args[0].clone();

	let mut opts = Options::new();
	opts.optflag("h", "help", "print this help menu");
	opts.optflag("v", "version", "print version and exit");
	opts.optopt("", "pg-dump-binary", "use the pg_dump binary in PG_DUMP_PATH", "PG_DUMP_PATH");

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

	if matches.free.len() < 2 {
		print_usage(std::io::stderr(), &program);
		process::exit(1);
	}

	let conninfo = matches.free.remove(0);
	let output_dir = OsString::from(matches.free.remove(0));
	if matches.free.len() > 0 {
		print_usage(std::io::stderr(), &program);
		process::exit(1);
	}

	let output_dir_path = Path::new(&output_dir);
	if output_dir_path.exists() {
		eprintln!("output directory already exists");
		process::exit(1);
	}

	let pg_conn = postgres::Client::connect(&conninfo, postgres::NoTls);
	if let Err(e) = pg_conn {
		eprintln!("could not connect to postgres: {}", e);
		process::exit(1);
	}
	let mut pg_conn = pg_conn.unwrap();

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

	if let Err(err) = fs::create_dir(&output_dir_path) {
		eprintln!("could not create output directory: {}", err);
		process::exit(1);
	}

	for (file, contents) in &dump.files {
		let path = output_dir_path.join(file);

		if let Some(parent) = path.parent() {
			if let Err(err) = fs::create_dir_all(&parent) {
				eprintln!("could not create output directory {}: {}", parent.display(), err);
				process::exit(1);
			}
		}

		let mut file = match File::create(&path) {
			Err(err) => {
				eprintln!("could not create output file {}: {}", path.display(), err);
				process::exit(1);
			},
			Ok(file) => file,
		};
		for line in contents {
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
	Ok(())
}
