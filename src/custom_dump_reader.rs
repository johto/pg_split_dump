use std::collections::HashMap;
use std::io::{self, BufReader, Read};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::QueriedDatabaseData;

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

// It would be nicer if we added custom structs for everything instead of
// (ab)using CustomDumpItem, but I'm too lazy to do that now.
#[derive(Debug)]
pub struct CustomDump {
	pub set_client_encoding: Option<String>,
	pub set_standard_conforming_strings: Option<String>,
	pub set_search_path: Option<String>,

	// The hash key is the file name, e.g. "public/FUNCTIONS/hello.sql".
	pub files: HashMap<String, Vec<String>>,
	pub file_order: Vec<String>,

	// List of pg_class entries which are views.  We need to keep track of these
	// so we know to put the ACLs for views into the right files.
	views: HashMap<View, ()>,
}

pub fn read_dump<R: Read>(input: R, queried_data: &QueriedDatabaseData) -> Result<CustomDump, DumpReadError> {
	let reader = CustomDumpReader::new(input)?;

	let mut dump = CustomDump::new();
	for item in reader.contents() {
		let item = item?;

		dump.add_item(item, queried_data)?;
	}

	Ok(dump)
}

impl CustomDump {
	fn new() -> CustomDump {
		CustomDump{
			set_client_encoding: None,
			set_standard_conforming_strings: None,
			set_search_path: None,

			files: HashMap::new(),
			file_order: vec![],
			views: HashMap::new(),
		}
	}

	fn add_item(&mut self, item: CustomDumpItem, queried_data: &QueriedDatabaseData) -> Result<(), DumpReadError> {
		fn other_error<S: Into<String>>(err: S) -> Result<(), DumpReadError> {
			return Err(DumpReadError::OtherError(err.into()));
		}

		if item.table_oid == 1262 && item.desc == "DATABASE" {
			// Won't be needing this guy.
			return Ok(());
		}

		let filename;
		let mut contents = vec![item.definition.clone()];

		let namespace = item.namespace.clone();

		match (item.table_oid, item.desc.as_ref()) {
			(0, "ENCODING") => {
				if self.set_client_encoding.is_some() {
					return other_error(r#"more than one "ENCODING" item present"#);
				}
				self.set_client_encoding = Some(item.definition.clone());
				filename = "index.sql".to_string();
			},
			(0, "STDSTRINGS") => {
				if self.set_standard_conforming_strings.is_some() {
					return other_error(r#"more than one "STDSTRINGS" item present"#);
				}
				self.set_standard_conforming_strings = Some(item.definition.clone());

				contents.push("SET check_function_bodies = false;\n".to_string());

				filename = "index.sql".to_string();
			},
			(0, "SEARCHPATH") => {
				if self.set_search_path.is_some() {
					return other_error(r#"more than one "SEARCHPATH" item present"#);
				}
				self.set_search_path = Some(item.definition.clone());
				filename = "index.sql".to_string();
			},
			(0, "ACL") => {
				filename = self.get_filename_from_combo_tag(&item, "ACL");
			},
			(0, "COMMENT") => {
				filename = self.get_filename_from_combo_tag(&item, "COMMENT");
			},
			(2615, "SCHEMA") => {
				filename = format!("SCHEMAS/{}.sql", item.tag);
			},
			(3079, "EXTENSION") => {
				filename = format!("EXTENSIONS/{}.sql", item.tag);
			},
			(0, "SHELL TYPE") => {
				filename = format!("{}/SHELL_TYPES/{}.sql", &item.namespace, item.tag);
			},
			(1247, "TYPE") => {
				filename = format!("{}/TYPES/{}.sql", &item.namespace, item.tag);
			}
			(1247, "DOMAIN") => {
				filename = format!("{}/DOMAINS/{}.sql", namespace, item.tag);
			},
			(1255, "FUNCTION") => {
				let function_name = item.tag.split_once("(").unwrap().0;
				filename = format!("{}/FUNCTIONS/{}.sql", &item.namespace, function_name);
			},
			(1255, "AGGREGATE") => {
				let function_name = item.tag.split_once("(").unwrap().0;
				filename = format!("{}/FUNCTIONS/{}.sql", &item.namespace, function_name);
			},
			(2617, "OPERATOR") => {
				filename = format!("{}/operators.sql", &item.namespace);
			},
			(1259, "TABLE") => {
				filename = format!("{}/TABLES/{}.sql", &item.namespace, &item.tag);
				contents.push(
					format!(
						"ALTER TABLE {}.{} OWNER TO {};\n",
						&item.namespace,
						&item.tag,
						&item.owner,
					),
				);
			},
			(1259, "INDEX") => {
				let table_name = queried_data.index_table.get(&item.oid).unwrap();
				filename = format!("{}/TABLES/{}.sql", &item.namespace, table_name);
			},
			(2606, "CONSTRAINT") => {
				let table_name = item.tag.split_once(" ").unwrap().0;
				filename = format!("{}/TABLES/{}.sql", &item.namespace, table_name);
			},
			(2606, "CHECK CONSTRAINT") => {
				let table_name = item.tag.split_once(" ").unwrap().0;
				filename = format!("{}/TABLES/{}.sql", &item.namespace, table_name);
			},
			(2604, "DEFAULT") => {
				let table_name = item.tag.split_once(" ").unwrap().0;
				filename = format!("{}/TABLES/{}.sql", namespace, table_name);
			},
			(2620, "TRIGGER") => {
				let table_name = item.tag.split_once(" ").unwrap().0;
				filename = format!("{}/TABLES/{}.sql", namespace, table_name);
			},
			(2606, "FK CONSTRAINT") => {
				let table_name = item.tag.split_once(" ").unwrap().0;
				filename = format!("{}/FK_CONSTRAINTS/{}.sql", namespace, table_name);
			},
			(1259, "SEQUENCE") => {
				filename = format!("{}/SEQUENCES/{}.sql", &item.namespace, item.tag);

				contents.push(
					format!(
						"ALTER SEQUENCE {}.{} OWNER TO {};\n",
						&item.namespace,
						&item.tag,
						&item.owner,
					),
				);
			},
			(0, "SEQUENCE OWNED BY") => {
				filename = format!("{}/SEQUENCES/{}.sql", &item.namespace, item.tag);
			},
			(1259, "VIEW") => {
				let hash_entry = View{
					schema: item.namespace.clone(),
					name: item.tag.clone(),
				};
				self.views.insert(hash_entry, ());

				filename = format!("{}/VIEWS/{}.sql", namespace, item.tag);
				contents = vec![
					format!("CREATE OR REPLACE VIEW {} AS", item.tag),
					queried_data.pretty_printed_views.get(&item.oid).unwrap().to_string(),
				];

				contents.push(
					format!(
						"ALTER VIEW {}.{} OWNER TO {};\n",
						&item.namespace,
						&item.tag,
						&item.owner,
					),
				);
			},
			(2618, "RULE") => {
				filename = format!("{}/RULES/{}.sql", &item.namespace, item.tag);
			},
			_ => {
				panic!("unknown table_oid / desc for item {:?}", item);
			},
		}

		match self.files.get_mut(&filename) {
			None => {
				self.files.insert(filename.clone(), contents);

				if filename != "index.sql" {
					self.files.get_mut("index.sql").unwrap().push(format!("\\i {}", &filename));
				}
			},
			Some(vec) => {
				vec.append(&mut contents);
			},
		};

		Ok(())
	}

	// A "combo tag", e.g. "SCHEMA public".
	fn get_filename_from_combo_tag(&mut self, item: &CustomDumpItem, typ: &str) -> String {
		let parts = item.tag.split_once(" ");
		let (desc, rest) = match parts {
			None => {
				panic!("invalid tag {:?}", item.tag);
			},
			Some(tup) => tup,
		};

		match desc {
			"SCHEMA" => {
				return format!("SCHEMAS/{}.sql", rest);
			},
			"EXTENSION" => {
				return format!("EXTENSIONS/{}.sql", rest);
			},
			"TYPE" => {
				return format!("{}/TYPES/{}.sql", &item.namespace, rest);
			},
			"FUNCTION" => {
				let function_name = rest.split_once("(").unwrap().0;
				return format!("{}/FUNCTIONS/{}.sql", &item.namespace, function_name);

			},
			"TABLE" => {
				if self.is_view(&item.namespace, rest) {
					return format!("{}/VIEWS/{}.sql", &item.namespace, rest);
				} else {
					return format!("{}/TABLES/{}.sql", &item.namespace, rest);
				}
			},
			"COLUMN" => {
				let table_name = rest.split_once(".").unwrap().0;
				return format!("{}/TABLES/{}.sql", &item.namespace, table_name);
			},
			"SEQUENCE" => {
				return format!("{}/SEQUENCES/{}.sql", &item.namespace, rest);
			},
			"VIEW" => {
				return format!("{}/SEQUENCES/{}.sql", &item.namespace, rest);
			},
			_ => {
				panic!("unknown desc {} for {} item {:?}", desc, typ, item);
			},
		};
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

		let _compression = self.read_int()?;
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
	_major_version: u8,
	_minor_version: u8,
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
			_major_version: major_version,
			_minor_version: minor_version,
			_revision: revision,
			int_size: int_size as usize,
			off_size: off_size as usize,
			_format: format,
		};
		Ok(header)
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
