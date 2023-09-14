use std::process;

// Creates a new configuration based off the supplied conninfo string.
// Unfortunately there's no way to load defaults from the environment à la
// libpq.  We try to support the important ones here at least.
pub fn create(conninfo: &str) -> postgres::config::Config {
	let mut pg_config = match conninfo.parse::<postgres::config::Config>() {
		Err(err) => {
			eprintln!("foo {}", err);
			process::exit(1);
		},
		Ok(pg_config) => pg_config,
	};

	if pg_config.get_user().is_none() {
		if let Ok(user) = std::env::var("PGUSER") {
			pg_config.user(&user);
		} else {
			eprintln!("database username must be specified");
			process::exit(1);
		}
	}

	if pg_config.get_dbname().is_none() {
		if let Ok(dbname) = std::env::var("PGDATABASE") {
			pg_config.dbname(&dbname);
		}
	}

	if pg_config.get_hosts().len() == 0 {
		if let Ok(host) = std::env::var("PGHOST") {
			pg_config.host(&host);
		}
	}

	if pg_config.get_ports().len() == 0 {
		if let Ok(portstr) = std::env::var("PGPORT") {
			let port = portstr.parse::<u16>();
			if let Err(_err) = port {
				eprintln!("invalid integer value {:?} for connection option \"port\"", portstr);
				process::exit(1);
			};

			pg_config.port(port.unwrap());
		};
	}

	if pg_config.get_options().is_none() {
		if let Ok(options) = std::env::var("PGOPTIONS") {
			pg_config.options(&options);
		}
	}

	pg_config
}
