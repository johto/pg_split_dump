use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::{self, BufReader, Read};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::auxiliary_data::AuxiliaryData;

#[derive(Debug)]
pub enum DumpReadError {
	IOError(io::Error),
	OtherError(String),
}

impl From<io::Error> for DumpReadError {
	fn from(error: io::Error) -> Self {
		Self::IOError(error)
	}
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct View {
	pub schema: String,
	pub name: String,
}

#[derive(Debug)]
pub struct SplitDumpDirectory {
	pub dirs: HashMap<String, Self>,
	pub files: HashMap<String, Vec<String>>,
}

impl SplitDumpDirectory {
	pub fn new() -> SplitDumpDirectory {
		SplitDumpDirectory{
			dirs: HashMap::new(),
			files: HashMap::new(),
		}
	}
}

// It would be nicer if we added custom structs for everything instead of
// (ab)using CustomDumpItem, but I'm too lazy to do that now.
#[derive(Debug)]
pub struct CustomDump {
	pub set_client_encoding: Option<String>,
	pub set_standard_conforming_strings: Option<String>,
	pub set_search_path: Option<String>,

	pub split_root: SplitDumpDirectory,
	pub file_order: Vec<String>,

	// List of pg_class entries which are views.  We need to keep track of these
	// so we know to put the ACLs for views into the right files.
	views: HashMap<View, ()>,
}

pub fn read_dump<R: Read>(input: R, aux_data: &AuxiliaryData) -> Result<CustomDump, DumpReadError> {
	let reader = CustomDumpReader::new(input)?;

	let mut dump = CustomDump::new();
	for item in reader.contents() {
		let item = item?;

		dump.add_item(item, aux_data)?;
	}

	Ok(dump)
}

impl CustomDump {
	fn new() -> CustomDump {
		CustomDump{
			set_client_encoding: None,
			set_standard_conforming_strings: None,
			set_search_path: None,

			split_root: SplitDumpDirectory::new(),
			file_order: vec![],
			views: HashMap::new(),
		}
	}

	fn add_item(&mut self, item: CustomDumpItem, aux_data: &AuxiliaryData) -> Result<(), DumpReadError> {
		fn other_error<S: Into<String>>(err: S) -> Result<(), DumpReadError> {
			return Err(DumpReadError::OtherError(err.into()));
		}

		if item.table_oid == 1262 && item.desc == "DATABASE" {
			// Won't be needing this guy.
			return Ok(());
		}

		let mut contents = vec![item.definition.clone()];
		let mut filepath;

		match (item.table_oid, item.desc.as_ref()) {
			(0, "ENCODING") => {
				if self.set_client_encoding.is_some() {
					return other_error(r#"more than one "ENCODING" item present"#);
				}
				self.set_client_encoding = Some(item.definition.clone());
				filepath = vec!["index.sql".to_string()];
			},
			(0, "STDSTRINGS") => {
				if self.set_standard_conforming_strings.is_some() {
					return other_error(r#"more than one "STDSTRINGS" item present"#);
				}
				self.set_standard_conforming_strings = Some(item.definition.clone());

				contents.push("SET check_function_bodies = false;\n".to_string());

				filepath = vec!["index.sql".to_string()];
			},
			(0, "SEARCHPATH") => {
				if self.set_search_path.is_some() {
					return other_error(r#"more than one "SEARCHPATH" item present"#);
				}
				self.set_search_path = Some(item.definition.clone());

				filepath = vec!["index.sql".to_string()];
			},
			(0, "ACL") => {
				contents = self.sort_acl(&item.definition);

				filepath = self.get_filepath_from_combo_tag(&item, "ACL");
			},
			(0, "COMMENT") => {
				filepath = self.get_filepath_from_combo_tag(&item, "COMMENT");
			},
			(2615, "SCHEMA") => {
				if item.tag == "public" {
					filepath = vec![];
				} else {
					filepath = vec![
						"SCHEMAS".to_string(),
						format!("{}.sql", &item.tag),
					];
				}
			},
			(3079, "EXTENSION") => {
				filepath = vec![
					"EXTENSIONS".to_string(),
					format!("{}.sql", &item.tag),
				];
			},
			(0, "SHELL TYPE") => {
				filepath = vec![
					item.namespace,
					"SHELL_TYPES".to_string(),
					format!("{}.sql", &item.tag),
				];
			},
			(1247, "TYPE") => {
				filepath = vec![
					item.namespace,
					"TYPES".to_string(),
					format!("{}.sql", &item.tag),
				];
			}
			(1247, "DOMAIN") => {
				filepath = vec![
					item.namespace,
					"DOMAINS".to_string(),
					format!("{}.sql", &item.tag),
				];
			},
			(1255, "FUNCTION") => {
				let subdir;
				if aux_data.trigger_functions.get(&item.oid).is_some() {
					subdir = "TRIGGER_FUNCTIONS";
				} else {
					subdir = "FUNCTIONS";
				}

				contents.push(
					format!(
						"ALTER FUNCTION {}.{} OWNER TO {};\n",
						&item.namespace,
						&item.tag,
						&item.owner,
					),
				);

				let function_name = item.tag.split_once("(").unwrap().0;

				filepath = vec![
					item.namespace,
					subdir.to_string(),
					format!("{}.sql", &function_name),
				];
			},
			(1255, "AGGREGATE") => {
				let function_name = item.tag.split_once("(").unwrap().0;
				filepath = vec![
					item.namespace,
					"FUNCTIONS".to_string(),
					format!("{}.sql", &function_name),
				];
			},
			(2617, "OPERATOR") => {
				filepath = vec![
					item.namespace,
					"operators.sql".to_string(),
				];
			},
			(1259, "TABLE") => {
				contents.push(
					format!(
						"ALTER TABLE {}.{} OWNER TO {};\n",
						&item.namespace,
						&item.tag,
						&item.owner,
					),
				);

				filepath = vec![
					item.namespace,
					"TABLES".to_string(),
					format!("{}.sql", &item.tag),
				];
			},
			(1259, "INDEX") => {
				let table_name = aux_data.index_table.get(&item.oid).unwrap();
				filepath = vec![
					item.namespace,
					"TABLES".to_string(),
					format!("{}.sql", &table_name),
				];
			},
			(2606, "CONSTRAINT") => {
				let table_name = item.tag.split_once(" ").unwrap().0;
				filepath = vec![
					item.namespace,
					"TABLES".to_string(),
					format!("{}.sql", &table_name),
				];
			},
			(2606, "CHECK CONSTRAINT") => {
				let table_name = item.tag.split_once(" ").unwrap().0;
				filepath = vec![
					item.namespace,
					"TABLES".to_string(),
					format!("{}.sql", &table_name),
				];
			},
			(2604, "DEFAULT") => {
				let table_name = item.tag.split_once(" ").unwrap().0;
				filepath = vec![
					item.namespace,
					"TABLES".to_string(),
					format!("{}.sql", &table_name),
				];
			},
			(2620, "TRIGGER") => {
				let table_name = item.tag.split_once(" ").unwrap().0;
				filepath = vec![
					item.namespace,
					"TABLES".to_string(),
					format!("{}.sql", &table_name),
				];
			},
			(2606, "FK CONSTRAINT") => {
				let table_name = item.tag.split_once(" ").unwrap().0;
				filepath = vec![
					item.namespace,
					"FK_CONSTRAINTS".to_string(),
					format!("{}.sql", &table_name),
				];
			},
			(1259, "SEQUENCE") => {
				contents.push(
					format!(
						"ALTER SEQUENCE {}.{} OWNER TO {};\n",
						&item.namespace,
						&item.tag,
						&item.owner,
					),
				);

				filepath = vec![
					item.namespace,
					"SEQUENCES".to_string(),
					format!("{}.sql", &item.tag),
				];
			},
			(0, "SEQUENCE OWNED BY") => {
				filepath = vec![
					item.namespace,
					"SEQUENCES".to_string(),
					format!("{}.sql", &item.tag),
				];
			},
			(1259, "VIEW") => {
				let hash_entry = View{
					schema: item.namespace.clone(),
					name: item.tag.clone(),
				};
				self.views.insert(hash_entry, ());

				contents = vec![
					format!("CREATE OR REPLACE VIEW {} AS", item.tag),
					aux_data.pretty_printed_views.get(&item.oid).unwrap().to_string(),
				];

				contents.push(
					format!(
						"ALTER VIEW {}.{} OWNER TO {};\n",
						&item.namespace,
						&item.tag,
						&item.owner,
					),
				);

				filepath = vec![
					item.namespace,
					"VIEWS".to_string(),
					format!("{}.sql", &item.tag),
				];
			},
			(2618, "RULE") => {
				filepath = vec![
					item.namespace,
					"RULES".to_string(),
					format!("{}.sql", &item.tag),
				];
			},
			(6104, "PUBLICATION") => {
				filepath = vec![
					"PUBLICATIONS".to_string(),
					item.tag.to_string(),
					format!("{}.sql", &item.tag),
				];
			},
			(6106, "PUBLICATION TABLE") => {
				let (publication_name, table_name) = item.tag.split_once(" ").unwrap();

				filepath = vec![
					"PUBLICATIONS".to_string(),
					publication_name.to_string(),
					format!("{}.sql", &table_name),
				];
			},
			_ => {
				panic!("unknown table_oid / desc for item {:?}", item);
			},
		}

		if filepath.len() >= 1 {
			let filepath_str = filepath.join("/");
			let filename = filepath.pop().unwrap();
			let mut cwd = &mut self.split_root;
			for dir in filepath.iter() {
				if cwd.dirs.get(dir).is_none() {
					cwd.dirs.insert(dir.clone(), SplitDumpDirectory::new());
				}
				cwd = cwd.dirs.get_mut(dir).unwrap();
			}
			match cwd.files.get_mut(&filename) {
				None => {
					cwd.files.insert(filename.clone(), contents);

					if filename != "index.sql" {
						self.split_root.files.get_mut("index.sql").unwrap().push(format!("\\ir {}", &filepath_str));
					}
				},
				Some(vec) => {
					vec.append(&mut contents);
				},
			};
		}

		Ok(())
	}

	// A "combo tag", e.g. "SCHEMA public".
	fn get_filepath_from_combo_tag(&mut self, item: &CustomDumpItem, typ: &str) -> Vec<String> {
		let parts = item.tag.split_once(" ");
		let (desc, rest) = match parts {
			None => {
				panic!("invalid tag {:?}", item.tag);
			},
			Some(tup) => tup,
		};

		match desc {
			"SCHEMA" => {
				return vec![
					"SCHEMAS".to_string(),
					format!("{}.sql", rest),
				];
			},
			"EXTENSION" => {
				return vec![
					"EXTENSIONS".to_string(),
					format!("{}.sql", rest),
				];
			},
			"TYPE" => {
				return vec![
					item.namespace.clone(),
					"TYPES".to_string(),
					format!("{}.sql", rest),
				];
			},
			"FUNCTION" => {
				let function_name = rest.split_once("(").unwrap().0;
				return vec![
					item.namespace.clone(),
					"FUNCTIONS".to_string(),
					format!("{}.sql", function_name),
				];
			},
			"TABLE" => {
				// ACLs don't know whether they're for a table or a view, so we
				// need to figure that out here.
				let subdir;
				if self.is_view(&item.namespace, rest) {
					subdir = "VIEWS";
				} else {
					subdir = "TABLES";
				}
				return vec![
					item.namespace.clone(),
					subdir.to_string(),
					format!("{}.sql", rest),
				];
			},
			"COLUMN" => {
				let table_name = rest.split_once(".").unwrap().0;
				return vec![
					item.namespace.clone(),
					"TABLES".to_string(),
					format!("{}.sql", table_name),
				];
			},
			"SEQUENCE" => {
				return vec![
					item.namespace.clone(),
					"SEQUENCES".to_string(),
					format!("{}.sql", rest),
				];
			},
			"VIEW" => {
				return vec![
					item.namespace.clone(),
					"VIEWS".to_string(),
					format!("{}.sql", rest),
				];
			},
			_ => {
				panic!("unknown desc {} for {} item {:?}", desc, typ, item);
			},
		};
	}

	// Sorts a string of ACL entries.  The unsorted order can be difficult to
	// predict, and is very annoying if you e.g. want to compare a live
	// database against one from version control.
	fn sort_acl(&self, acl: &str) -> Vec<String> {
		let mut parts = vec![];
		for entry in acl.split(";\n") {
			if entry == "" {
				continue;
			}
			parts.push(entry.to_string() + ";");
		}

		parts.sort_unstable_by(|a, b| {
			let revoke_grant = a.starts_with("REVOKE").partial_cmp(&b.starts_with("REVOKE")).unwrap();
			if revoke_grant != Ordering::Equal {
				// REVOKE before GRANT
				return revoke_grant.reverse();
			}
			return a.partial_cmp(b).unwrap();
		});

		// Keep the extra empty line at the end.
		parts.push(String::new());

		return parts;
	}

	fn is_view(&self, schema: &str, pg_class_entry: &str) -> bool {
		let hash_entry = View{
			schema: schema.to_string(),
			name: pg_class_entry.to_string(),
		};
		return self.views.get(&hash_entry).is_some();
	}
}

#[derive(Debug)]
struct CustomDumpReader<R: Read> {
	reader: BufReader<R>,
	static_header: CustomDumpStaticHeader,
	header: Option<CustomDumpHeader>,
}

impl<R> CustomDumpReader<R>
where
	R: Read,
{
	fn new(input: R) -> Result<CustomDumpReader<R>, DumpReadError> {
		let mut reader = BufReader::new(input);

		let static_header = match CustomDumpStaticHeader::read(&mut reader) {
			Err(err) => {
				return Err(DumpReadError::OtherError(format!("could not read dump header: {}", err)));
			},
			Ok(header) => header,
		};

		if static_header.dump_version() >= (1, 12) &&
			static_header.dump_version() <= (1, 15) {
			// OK
		} else {
			return Err(
				DumpReadError::OtherError(
					format!(
						"unsupported dump version ({}.{})",
						static_header.major_version,
						static_header.minor_version,
					),
				),
			);
		}

		let mut reader = CustomDumpReader{
			reader: reader,
			static_header: static_header,
			header: None,
		};
		reader.header = Some(reader.read_header()?);
		Ok(reader)
	}

	fn read_header(&mut self) -> io::Result<CustomDumpHeader> {
		if self.header.is_some() {
			panic!("header already read");
		}

		if self.dump_version() >= (1, 15) {
			let _compression_algorithm = self.read_u8()?;
		} else {
			let _compression = self.read_int()?;
		}

		let _sec = self.read_int()?;
		let _min = self.read_int()?;
		let _hour = self.read_int()?;
		let _mday = self.read_int()?;
		let _mon = self.read_int()? + 1;
		let _year = self.read_int()? + 1900;
		let _isdst = self.read_int()?;
		let _dbname = self.read_str()?;
		let _remote_version = self.read_str()?;
		let _pg_dump_version = self.read_str()?;
		let num_items = self.read_int()?;
		Ok(CustomDumpHeader{
			num_items: num_items,
		})
	}

	fn contents(self) -> CustomDumpContentsIterator<R> {
		let num_items = self.header.as_ref().unwrap().num_items;
		CustomDumpContentsIterator{
			dump_reader: self,
			items_left: num_items,
		}
	}

	fn read_u8(&mut self) -> io::Result<u8> {
		self.reader.read_u8()
	}

	fn read_int(&mut self) -> io::Result<i64> {
		let sign = self.reader.read_u8()?;
		let mut int_value = self.reader.read_int::<LittleEndian>(self.static_header.int_size)?;
		if sign == 1 {
			int_value = -int_value;
		} else if sign != 0 {
			panic!("oops")
		}
		Ok(int_value)
	}

	fn read_offset(&mut self) -> io::Result<u64> {
		let _flag = self.reader.read_u8()?;
		self.reader.read_uint::<LittleEndian>(self.static_header.off_size)
	}

	fn read_str(&mut self) -> io::Result<String> {
		let len = self.read_int()?;
		if len <= 0 {
			return Ok("".to_string());
		}
		let mut v = Vec::with_capacity(len as usize);
		for _i in 0..len {
			v.push(self.reader.read_u8()?);
		}
		let s = String::from_utf8(v).expect("file must be valid UTF-8");
		Ok(s)
	}

	fn read_oid_str(&mut self) -> io::Result<u32> {
		let oid = self.read_str()?;
		return Ok(oid.parse::<u32>().unwrap());
	}

	fn dump_version(&self) -> (u8, u8) {
		self.static_header.dump_version()
	}

	fn read_item(&mut self) -> io::Result<CustomDumpItem> {
		let _dump_id = self.read_int()?;
		let _data_dumper = self.read_int()?;
		let table_oid = self.read_oid_str()?;
		let oid = self.read_oid_str()?;
		let tag = self.read_str()?;
		let desc = self.read_str()?;
		let _section = self.read_int()?;
		let definition = self.read_str()?;
		let _drop_stmt = self.read_str()?;
		let _copy_stmt = self.read_str()?;
		let namespace = self.read_str()?;
		let _tablespace = self.read_str()?;
		if self.dump_version() >= (1, 14) {
			let _tableam = self.read_str()?;
		}
		let owner = self.read_str()?;
		let _with_oids = self.read_str()?;

		loop {
			let _dep = self.read_str()?;
			if _dep == "" {
				break;
			}
		}
		let _offset = self.read_offset();

		Ok(CustomDumpItem{
			table_oid: table_oid,
			oid: oid,
			tag: tag,
			desc: desc,
			definition: definition,
			namespace: namespace,
			owner: owner,
		})
	}
}

#[derive(Debug)]
struct CustomDumpStaticHeader {
	major_version: u8,
	minor_version: u8,
	_revision: u8,
	int_size: usize,
	off_size: usize,
	_format: u8,
}

impl CustomDumpStaticHeader {
	fn read(reader: &mut impl std::io::Read) -> io::Result<CustomDumpStaticHeader> {
		let mut magic = [0; 5];
		reader.read_exact(&mut magic)?;

		let major_version = reader.read_u8()?;
		let minor_version = reader.read_u8()?;
		let revision = reader.read_u8()?;

		let int_size = reader.read_u8()?;
		let off_size = reader.read_u8()?;
		let format = reader.read_u8()?;

		let header = CustomDumpStaticHeader{
			major_version: major_version,
			minor_version: minor_version,
			_revision: revision,
			int_size: int_size as usize,
			off_size: off_size as usize,
			_format: format,
		};

		Ok(header)
	}

	fn dump_version(&self) -> (u8, u8) {
		(self.major_version, self.minor_version)
	}

}

#[derive(Debug)]
struct CustomDumpHeader {
	num_items: i64,
}

#[derive(Debug)]
pub struct CustomDumpItem {
	pub table_oid: u32,
	pub oid: u32,
	pub tag: String,
	pub desc: String,
	pub definition: String,
	pub namespace: String,
	pub owner: String,
}

#[derive(Debug)]
struct CustomDumpContentsIterator<R: Read> {
	dump_reader: CustomDumpReader<R>,
	items_left: i64,
}

impl<R> Iterator for CustomDumpContentsIterator<R>
where
	R: Read,
{
	type Item = io::Result<CustomDumpItem>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.items_left == 0 {
			// TODO Check for EOF and close the file properly
			return None;
		}
		let item = self.dump_reader.read_item();
		self.items_left -= 1;
		Some(item)
	}
}
