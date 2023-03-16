use std::ffi::OsStr;
use std::process;
use std::io::{self, BufRead, BufReader, Read};
use std::sync::mpsc::{Receiver, channel};
use std::thread;

pub struct PgDumpSubprocess {
	child_process: process::Child,
	stderr_reader: Option<thread::JoinHandle<Vec<String>>>,
	stderr_thread_error_channel: Receiver<String>,
	stdout: process::ChildStdout,
}

impl PgDumpSubprocess {
	pub fn new(pg_dump_binary_path: &OsStr, conninfo: &str, snapshot_id: &str) -> Result<PgDumpSubprocess, ()> {
		let child = process::Command::new(pg_dump_binary_path)
			.arg("--schema-only")
			.args(["--format", "custom"])
			.args([&OsStr::new("--snapshot"), &OsStr::new(snapshot_id)])
			.args([&OsStr::new("--dbname"), &OsStr::new(conninfo)])
			.stdin(process::Stdio::null())
			.stdout(process::Stdio::piped())
			.stderr(process::Stdio::piped())
			.spawn();
		if let Err(e) = child {
			panic!("could not start pg_split_dump: {}", e);
		}
		let mut child = child.unwrap();

		// We need to organize a background thread to read the stderr output or
		// there's a risk of deadlocking if the process decides to write a lot
		// of data into stderr.
		let stderr = child.stderr.take().unwrap();
		let (tx, rx) = channel();
		let stderr_reader = thread::spawn(move || {
			let mut lines = Vec::new();
			let br = BufReader::new(stderr);
			for line in br.lines() {
				if let Err(e) = line {
					let _ = tx.send(e.to_string());
					return Vec::new();
				}
				lines.push(line.unwrap());
			}
			return lines;
		});

		let stdout = child.stdout.take().unwrap();

		Ok(
			PgDumpSubprocess{
				child_process: child,
				stderr_reader: Some(stderr_reader),
				stderr_thread_error_channel: rx,
				stdout: stdout,
			},
		)
	}
}

impl Read for PgDumpSubprocess {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let res = self.stdout.read(buf);
		if res.is_err() {
			return res;
		}
		let read_len = res.unwrap();
		if read_len > 0 {
			// Check that the stderr thread hasn't failed.
			if let Ok(_) = self.stderr_thread_error_channel.try_recv() {
				panic!("poopoo");
			}
			return Ok(read_len);
		}

		// End of stream, we need to make sure the subprocess ran successfully
		// and clean up.
		let stderr_lines = self.stderr_reader.take().unwrap().join().unwrap();
		let exit_status = self.child_process.wait().expect("child process wasn't running");
		if !exit_status.success() {
			eprintln!("ERROR:  pg_dump failed with the following output:");
			eprintln!("");
			for line in stderr_lines {
				eprintln!("    {}", line);
			}
			// TODO: handle signals
			process::exit(1);
		}
		return Ok(0);
	}
}
