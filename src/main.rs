use std::io::Read;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::model::Object;
use aws_sdk_s3::types::{ByteStream};
use directories::ProjectDirs;

static mut MAX_BACKUP_SIZE: i64 = 5_000_000_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ProjectDirs::from("com", "Wolfpack", "backup-s3").unwrap();
    let config_dir = dir.config_dir();
    let config_file = config_dir.join("config.toml");
    let config_file_clone2 = config_file.clone();
    println!("Config file: {}", config_file.display());

    // Create directory if it doesn't exist
    if !config_dir.exists() {
        std::fs::create_dir_all(config_dir)?;
    }

    // Create config file if it doesn't exist
    if !config_file.exists() {
        let config_file_clone = config_file.clone();
        std::fs::File::create(config_file)?;
        // Write the contents from C:\Users\Kalka\CLionProjects\backup-s3\examples\config.example.toml to the config file
        let mut config_example_file = std::fs::File::open("examples/config.example.toml")?;
        let mut config_example_contents = String::new();
        config_example_file.read_to_string(&mut config_example_contents)?;
        std::fs::write(config_file_clone, config_example_contents)?;
    }

    // Read the config file as toml
    let config_file_contents = std::fs::read_to_string(config_file_clone2)?;
    let config: toml::Value = toml::from_str(&config_file_contents)?;

    // Check if the AWS credentials are set in the config file
    if config["aws"]["AWS_ACCESS_KEY_ID"] == toml::Value::String("<key here>".to_string()) {
        panic!("AWS_ACCESS_KEY_ID is not set in the config file");
    } else {
        // Set the AWS_ACCESS_KEY_ID environment variable
        std::env::set_var("AWS_ACCESS_KEY_ID", config["aws"]["AWS_ACCESS_KEY_ID"].as_str().unwrap());
    }

    if config["aws"]["AWS_SECRET_ACCESS_KEY"] == toml::Value::String("<secret here>".to_string()) {
        panic!("AWS_SECRET_ACCESS_KEY is not set in the config file");
    } else {
        std::env::set_var("AWS_SECRET_ACCESS_KEY", config["aws"]["AWS_SECRET_ACCESS_KEY"].as_str().unwrap());
    }

    if config["backups"]["backups_folder"] == toml::Value::String("".to_string()) {
        panic!("backups_folder is not set in the config file");
    } else {
        std::env::set_var("BACKUPS_FOLDER", config["backups"]["backups_folder"].as_str().unwrap());
    }

    if config["backups"]["max_backup_size"] != toml::Value::String("".to_string()) {
        unsafe {
            MAX_BACKUP_SIZE = config["backups"]["max_backup_size"].as_integer().unwrap();
        }
    }

    if !std::path::Path::new(config["backups"]["backups_folder"].as_str().unwrap()).exists() {
        panic!("backups_folder does not exist");
    }

    // Check if the backups folder is empty
    if std::fs::read_dir(config["backups"]["backups_folder"].as_str().unwrap()).unwrap().count() == 0 {
        panic!("backups_folder is empty");
    }

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let aws_config = aws_config::from_env()
        .region(region_provider)
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&aws_config);

    let resp = client.list_buckets().send().await?;
    println!("Buckets: {:#?}", resp.buckets.unwrap());

    // List objects in the 'backups' folder in the bucket 'wolfpackmc'
    let resp = client.list_objects_v2()
        .bucket("wolfpackmc")
        .prefix("backups")
        .send()
        .await?;

    let mut cum_size: i64 = 0;

    let mut saved_object: Option<Object> = None;
    let mut saved_last_modified: i64 = 0;

    // Get the latest backup file in config["backups"]["backups_folder"]
    let mut latest_backup_file: Option<std::fs::DirEntry> = None;
    let mut latest_backup_file_last_modified: u64 = 0;

    for entry in std::fs::read_dir(config["backups"]["backups_folder"].as_str().unwrap())? {
        let entry = entry?;
        if !entry.file_name().to_str().unwrap().ends_with(".zip") {
            continue;
        }
        let metadata = entry.metadata()?;
        let last_modified = metadata.modified().unwrap().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        if last_modified > latest_backup_file_last_modified {
            latest_backup_file = Some(entry);
            latest_backup_file_last_modified = last_modified;
        }
    }

    // Get the file name of the latest backup
    let latest_backup_file_some = latest_backup_file.unwrap();
    let latest_backup_file_name = latest_backup_file_some.file_name().into_string().unwrap();
    let latest_backup_file_path = latest_backup_file_some.path();

    for object in resp.contents.unwrap() {
        if object.key().unwrap() == format!("backups/wolfpackmc-{}", latest_backup_file_name) {
            println!("Latest backup already exists in the bucket");
            return Ok(())
        }
        // Check if the latest backup file already exists in the bucket
        println!("Object: {:#?}", object);
        // Find the oldest object
        let object_size = object.size();
        cum_size += object_size;
        // Get the oldest object from the list
        let last_modified = object.last_modified().unwrap().secs();
        if last_modified < saved_last_modified {
            saved_object = Some(object);
        }
        saved_last_modified = last_modified;
    }

    // check if saved_object is not null
    if saved_object.is_some() {
        let saved_object = saved_object.unwrap();
        println!("Oldest object: {:#?}", saved_object);
        if cum_size > MAX_BACKUP_SIZE {
            println!("Cumulative size of backups is greater than the maximum allowed size, deleting oldest backup");
            // Delete the oldest backup
            let resp = client.delete_object()
                .bucket("wolfpackmc")
                .key(saved_object.key().unwrap())
                .send()
                .await?;
            println!("Deleted oldest backup: {:#?}", resp);
        }
    }

    let body = ByteStream::from_path(latest_backup_file_path).await?;

    println!("Uploading file: {}", latest_backup_file_name);

    // Upload the latest backup to the bucket
    client.put_object()
        .bucket("wolfpackmc")
        .key(format!("backups/wolfpackmc-{}", latest_backup_file_name))
        .body(body)
        .send()
        .await?;

    println!("Uploaded file: {:#?}", latest_backup_file_name);


    Ok(())
}
