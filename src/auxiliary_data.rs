use std::collections::HashMap;

/// Auxiliary data queried from the database.
#[derive(Debug)]
pub struct AuxiliaryData {
	// Index oid -> table name.
	pub index_table: HashMap<u32, String>,
	// View oid -> definition.
	pub pretty_printed_views: HashMap<u32, String>,
	// Function oid.
	pub trigger_functions: HashMap<u32, ()>,
}

pub fn query(txn: &mut postgres::Transaction) -> Result<AuxiliaryData, String> {
	let mut aux = AuxiliaryData{
		index_table: HashMap::new(),
		pretty_printed_views: HashMap::new(),
		trigger_functions: HashMap::new(),
	};

	let rows = txn.query(
		"
			SELECT pg_index.indexrelid, pg_class.relname
			FROM pg_index
			JOIN pg_class ON pg_class.oid = pg_index.indrelid
		",
		&[],
	);
	let rows = match rows {
		Err(err) => {
			return Err(format!("could not query pg_index: {}", err));
		},
		Ok(rows) => rows,
	};
	for row in rows {
		let oid: u32 = row.get(0);
		let relname: String = row.get(1);
		if let Some(_relname) = aux.index_table.insert(oid, relname) {
			panic!("oid {} seen twice in pg_index", oid);
		}
	}

	let rows = txn.query(
		"
			SELECT pg_class.oid, pg_get_viewdef(pg_class.oid, true)
			FROM pg_class
			WHERE pg_class.relkind = 'v'
		",
		&[],
	);
	let rows = match rows {
		Err(err) => {
			return Err(format!("could not query pg_index: {}", err));
		},
		Ok(rows) => rows,
	};
	for row in rows {
		let oid: u32 = row.get(0);
		let view_definition: String = row.get(1);
		if let Some(_view_definition) = aux.pretty_printed_views.insert(oid, view_definition) {
			panic!("oid {} seen twice in pg_class", oid);
		}
	}

	let rows = txn.query(
		"
			SELECT pg_proc.oid
			FROM pg_proc
			WHERE
				pg_proc.prorettype = 2279
		",
		&[],
	);
	let rows = match rows {
		Err(err) => {
			return Err(format!("could not query pg_proc: {}", err));
		},
		Ok(rows) => rows,
	};
	for row in rows {
		let oid: u32 = row.get(0);
		if let Some(_relname) = aux.trigger_functions.insert(oid, ()) {
			panic!("oid {} seen twice in pg_proc", oid);
		}
	}

	Ok(aux)
}
