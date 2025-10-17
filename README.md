# list-feeds

This application is a service for generating and serving custom Bluesky feeds from one or more user lists. It consumes data from the Bluesky network via Jetstream, stores it in a SQLite database, and serves the custom feeds over an HTTP server compatible with the Bluesky API.

The service can generate two types of feeds for each configured list:

* **Chronological Feed**: A feed of all posts, reposts, and relevant replies from members of a list, sorted chronologically.
* **Popular Feed**: A feed of posts popular with list members, ranked using a time-decay algorithm similar to Hacker News.

## Configuration

The application is configured using a YAML file, typically located at `./data/config.yml`. A sample configuration is provided in `data/config-sample.yml`.

### Top-Level Configuration

| Key | Description |
| :--- | :--- |
| `service` | Configuration for the feed generator service itself. See below for details. |
| `jetstream_hosts` | (Optional) A list of custom Jetstream instance hostnames to connect to. Defaults to the official Bluesky instances if omitted. |
| `db` | Database configuration. See below for details. |
| `feeds` | A list of feed configurations to generate. See below for details. |

### `service` Block

| Key | Description |
| :--- | :--- |
| `host` | The public-facing hostname of your service (e.g., `feeds.example.com`). |
| `service_did` | (Optional) The DID your service will use. Defaults to `did:web:<host>` if not provided. |
| `max_age` | The maximum age of records (posts, likes, reposts) to keep in the database, in days. |
| `max_lag` | The maximum tolerated lag in seconds between the server and the Jetstream firehose before a "lag notice" post is pinned to the top of feeds. |

### `db` Block

| Key | Description |
| :--- | :--- |
| `type` | The database type. Currently only `sqlite` is supported. |
| `path` | The file path for the SQLite database. |
| `debug` | Set to `true` to enable verbose SQL query logging. |

### `feeds` Block

This is a list where each item defines a set of feeds for a specific Bluesky list.

| Key | Description |
| :--- | :--- |
| `list_uri` | The AT URI of the Bluesky list to generate feeds from. |
| `feed_did` | The DID of the Bluesky account that will publish the feed generator records. |
| `chronological` | Configuration for the chronological feed. |
| `popular` | Configuration for the popular feed. |

Both `chronological` and `popular` blocks share a base configuration:

| Key | Description |
| :--- | :--- |
| `enabled` | Set to `true` to enable this feed. |
| `name` | The display name of the feed on Bluesky. |
| `slug` | The record name (rkey) for the feed generator. This forms part of the feed's AT URI. |
| `avatar` | Path to an image file for the feed's avatar. |
| `description` | The description of the feed on Bluesky. |
| `lag_post` | The AT URI of a post to pin to the top of the feed if the service is experiencing significant lag. |

The `popular` block has an additional `weights` section:

| Key | Description |
| :--- | :--- |
| `likes` | The weight to apply for each like from a list member. |
| `replies` | The weight to apply for each reply from a list member. |
| `reposts` | The weight to apply for each repost from a list member. |
| `list_member` | A multiplier to boost the score of posts authored by list members. Defaults to 1.0. |
| `newness` | The "gravity" for the time-decay algorithm. Higher values boost newer posts more aggressively. 1.8 is standard. |
| `max_age` | The oldest posts to include in the feed, in hours. |

---

## Running the Application

### Using Docker (Recommended)

This is the easiest way to run the service.

1. **Create a `data` directory** on your host machine.
2. **Create and configure `config.yml`** inside the `data` directory. Copy the contents of `data/config-sample.yml` and modify it for your setup. Make sure all paths (like for the database and avatars) are relative to the `/data` directory inside the container (e.g., `./feed-data.db`).
3. **Run the container** using Docker Compose. A sample `compose-sample.yaml` is provided. Copy it to compose.yaml and modify if needed. Then run:

    ```sh
    docker compose up -d
    ```

### Compiling and Running Locally

If you prefer to run the application without Docker:

1. **Install Go** version 1.24 or later.
2. **Clone the repository**.
3. **Build the binary**:

    ```sh
    go build -o list-feeds ./cmd/list-feeds
    ```

4. **Create a `data` directory** and configure your `config.yml` inside it.
5. **Run the application**:

    ```sh
    ./list-feeds
    ```

    The application will look for the configuration file at `./data/config.yml` by default.

---

## Publishing Feeds

Before a feed can be discovered by Bluesky clients, a feed generator record must be published to the account that will own the feed. This is done using the `publish-feed` utility.

### Using Docker

If you are running the application with Docker, you can use `docker-compose` to run the `publish-feed` command within your running container setup.

1. Ensure your `compose.yaml` and your `data` directory (with a valid `config.yml`) are in your current directory.
2. Generate an app password for your Bluesky account.
3. Run the command, passing your app password using the -p flag. It will use the configuration from your mounted `/data` volume.

    ```sh
    docker compose run --rm bsky-list-feeds \
      /publish-feed -p "YOUR_APP_PASSWORD"
    ```

    * By default, this will publish all feeds in your `config.yml`. To publish only specific feeds, use the `-l` flag with the list URI.

### Compiling and Running Locally

If you are running the application from a local build:

1. **Build the utility**:

    ```sh
    go build -o publish-feed ./cmd/publish-feed
    ```

2. **Run the command**:

    ```sh
    ./publish-feed -p "YOUR_APP_PASSWORD"
    ```

    * The `-p` flag requires the password (preferably an app password) for the account specified by `feed_did` in your config.
    * Use the `-c` flag to specify a custom path to your configuration file.

---

## Development

To set up a local development environment:

1. **Install Go** (version 1.24+).
2. **Clone the repository**.
3. **Install dependencies**:

    ```sh
    go mod tidy
    ```

4. **Create `data/config.yml`** for your development setup.
5. **Run in Debug Mode**: If you are using VS Code, a debug configuration is provided in `.vscode/launch.json` that will run the `list-feeds` application with the debugger attached.

---

## License

This project is licensed under the Apache License 2.0.
