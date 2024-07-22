use std::io;
use std::fmt;
use std::fs::{self, File};
use std::path::Path;
use std::process;

use crate::custom_dump_reader::SplitDumpDirectory;

#[derive(Debug, Eq, PartialEq)]
pub enum OutputFormat {
	TarArchive,
	Directory,
}

impl OutputFormat {
	pub fn from_string(s: &str) -> Option<OutputFormat> {
		match s {
			"t" => Some(OutputFormat::TarArchive),
			"d" => Some(OutputFormat::Directory),
			_ => None,
		}
	}
}

#[derive(Debug)]
pub enum TarOutputError {
	IOError(io::Error),
	OtherError(String),
}

impl From<io::Error> for TarOutputError {
	fn from(error: io::Error) -> Self {
	    Self::IOError(error)
	}
}

impl fmt::Display for TarOutputError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			TarOutputError::IOError(err) => write!(f, "IOError: {}", err),
			TarOutputError::OtherError(err) => write!(f, "{}", err),
		}
	}
}

pub struct TarOutputWriter {
	output_filename: String,
	metadata: fs::Metadata,
	archive: tar::Builder<File>,
}

impl TarOutputWriter {
	pub fn new(output_path: &Path) -> Result<TarOutputWriter, TarOutputError> {
		let file = File::create(&output_path)?;
		let metadata = match file.metadata() {
			Err(err) => {
				let err = TarOutputError::OtherError(
					format!("could not get metadata for {}: {}", &output_path.display(), err),
				);
				return Err(err);
			},
			Ok(metadata) => metadata,
		};
		let archive = tar::Builder::new(file);

		Ok(TarOutputWriter{
			output_filename: output_path.to_string_lossy().into_owned(),
			metadata: metadata,
			archive: archive,
		})
	}

	pub fn write_from_split_dump(self: &mut TarOutputWriter, contents: &SplitDumpDirectory) -> Result<(), TarOutputError> {
		self.write_directory(Path::new(""), contents)
	}

	fn write_file(self: &mut TarOutputWriter, parent: &Path, filename: &str, contents: &str) -> Result<(), TarOutputError> {
		let path = parent.join(filename);

		let mut header = tar::Header::new_gnu();
		header.set_metadata(&self.metadata);
		header.set_size(contents.len() as u64);
		header.set_cksum();

		self.archive.append_data(&mut header, &path, contents.as_bytes())?;
		Ok(())
	}

	fn write_directory(self: &mut TarOutputWriter, path: &Path, contents: &SplitDumpDirectory) -> Result<(), TarOutputError> {
		for (filename, file_contents) in &contents.files {
			let materialized = file_contents.join("\n") + "\n";
			self.write_file(path, &filename, &materialized)?;
		}
		for (subdir, subdir_contents) in &contents.dirs {
			let subdir_path = path.join(subdir);
			self.write_directory(&subdir_path, subdir_contents)?
		}
		Ok(())
	}

	pub fn sync(self: TarOutputWriter) {
		let file = match self.archive.into_inner() {
			Err(err) => {
				eprintln!("could not write output file {}: {}", self.output_filename, err);
				process::exit(1);
			},
			Ok(file) => file,
		};
		if let Err(err) = file.sync_all() {
			eprintln!("could not write output file {}: {}", self.output_filename, err);
			process::exit(1);
		}
	}
}
