use clap::{Parser, Subcommand};
use libsql::{Builder, Connection};
use rayon::prelude::*;
use std::io::{self, Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

#[derive(Parser)]
#[command(name = "cli")]
#[command(about = "Advent of Code CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Insert or update a solution (input read from stdin)
    Upsert {
        #[arg(short, long)]
        year: u16,
        #[arg(short, long)]
        day: u8,
        #[arg(short, long)]
        part: u8,
        #[arg(short, long)]
        solution: String,
    },
    /// Read the solution/output for a given year, day, part
    ReadSolution {
        #[arg(short, long)]
        year: u16,
        #[arg(short, long)]
        day: u8,
        #[arg(short, long)]
        part: u8,
    },
    /// Read the input for a given year, day, part
    ReadInput {
        #[arg(short, long)]
        year: u16,
        #[arg(short, long)]
        day: u8,
        #[arg(short, long)]
        part: u8,
    },
    /// Delete a solution
    Delete {
        #[arg(short, long)]
        year: u16,
        #[arg(short, long)]
        day: u8,
        #[arg(short, long)]
        part: u8,
    },
    /// List all solutions
    List,
    /// Run a solution and verify against expected output
    Run {
        #[arg(short, long)]
        year: u16,
        #[arg(short, long)]
        day: u8,
        #[arg(short, long)]
        part: u8,
        /// Number of runs for timing (default: 10)
        #[arg(short, long, default_value_t = 10)]
        runs: u32,
    },
    /// Run all solutions and verify against expected outputs
    RunAll {
        /// Number of runs for timing (default: 10)
        #[arg(short, long, default_value_t = 10)]
        runs: u32,
    },
    /// Initialize the database (create tables)
    Init,
    /// Reset the database (delete all data)
    Reset,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let db = Builder::new_local("aoc.db").build().await?;
    let conn = db.connect()?;

    match cli.command {
        Commands::Upsert {
            year,
            day,
            part,
            solution,
        } => {
            let mut input = String::new();
            io::stdin().read_to_string(&mut input)?;
            upsert_solution(&conn, year, day, part, &input, &solution).await?;
            println!("Upserted: year={year}, day={day}, part={part}, solution={solution}");
        }
        Commands::ReadSolution { year, day, part } => {
            let solution = read_solution(&conn, year, day, part).await?;
            print!("{solution}");
        }
        Commands::ReadInput { year, day, part } => {
            let input = read_input(&conn, year, day, part).await?;
            print!("{input}");
        }
        Commands::Delete { year, day, part } => {
            delete_solution(&conn, year, day, part).await?;
            println!("Deleted: year={year}, day={day}, part={part}");
        }
        Commands::List => {
            list_solutions(&conn).await?;
        }
        Commands::Run { year, day, part, runs } => {
            let result = run_solution(&conn, year, day, part, runs).await?;
            print_run_result(&result);
            if !result.passed {
                std::process::exit(1);
            }
        }
        Commands::RunAll { runs } => {
            let summary = run_all_solutions(&conn, runs).await?;
            println!(
                "\n{GREEN}✓ {} passed{RESET}, {RED}✗ {} errors{RESET}, {YELLOW}⧖ {} timeouts{RESET} ({:.2}ms)",
                summary.passed,
                summary.errors,
                summary.timeouts,
                summary.total_time_ms as f64
            );
            if summary.errors > 0 || summary.timeouts > 0 {
                std::process::exit(1);
            }
        }
        Commands::Init => {
            create_schema(&conn).await?;
            println!("Database initialized");
        }
        Commands::Reset => {
            reset_db(&conn).await?;
            println!("Database reset");
        }
    }

    Ok(())
}

async fn create_schema(conn: &Connection) -> Result<(), libsql::Error> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS solutions (
            year INTEGER NOT NULL,
            day INTEGER NOT NULL,
            part INTEGER NOT NULL,
            input TEXT NOT NULL,
            output TEXT NOT NULL,
            PRIMARY KEY (year, day, part)
        )",
        (),
    )
    .await?;
    Ok(())
}

async fn reset_db(conn: &Connection) -> Result<(), libsql::Error> {
    conn.execute("DROP TABLE IF EXISTS solutions", ()).await?;
    Ok(())
}

async fn upsert_solution(
    conn: &Connection,
    year: u16,
    day: u8,
    part: u8,
    input: &str,
    output: &str,
) -> Result<(), libsql::Error> {
    conn.execute(
        "INSERT OR REPLACE INTO solutions (year, day, part, input, output) VALUES (?, ?, ?, ?, ?)",
        (year, day, part, input, output),
    )
    .await?;
    Ok(())
}

async fn read_solution(
    conn: &Connection,
    year: u16,
    day: u8,
    part: u8,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut rows = conn
        .query(
            "SELECT output FROM solutions WHERE year = ? AND day = ? AND part = ?",
            (year, day, part),
        )
        .await?;
    if let Some(row) = rows.next().await? {
        Ok(row.get::<String>(0)?)
    } else {
        Err(format!("No solution found for year={year}, day={day}, part={part}").into())
    }
}

async fn read_input(
    conn: &Connection,
    year: u16,
    day: u8,
    part: u8,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut rows = conn
        .query(
            "SELECT input FROM solutions WHERE year = ? AND day = ? AND part = ?",
            (year, day, part),
        )
        .await?;
    if let Some(row) = rows.next().await? {
        Ok(row.get::<String>(0)?)
    } else {
        Err(format!("No input found for year={year}, day={day}, part={part}").into())
    }
}

async fn delete_solution(conn: &Connection, year: u16, day: u8, part: u8) -> Result<(), libsql::Error> {
    conn.execute(
        "DELETE FROM solutions WHERE year = ? AND day = ? AND part = ?",
        (year, day, part),
    )
    .await?;
    Ok(())
}

async fn list_solutions(conn: &Connection) -> Result<(), libsql::Error> {
    let mut rows = conn
        .query(
            "SELECT year, day, part, output FROM solutions ORDER BY year, day, part",
            (),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let year: u16 = row.get::<u32>(0)? as u16;
        let day: u8 = row.get::<u32>(1)? as u8;
        let part: u8 = row.get::<u32>(2)? as u8;
        let output: String = row.get(3)?;
        println!("{year}\t{day}\t{part}\t{output}");
    }
    Ok(())
}

struct TimingStats {
    min_ms: f64,
    max_ms: f64,
    mean_ms: f64,
}

struct RunResult {
    year: u16,
    day: u8,
    part: u8,
    passed: bool,
    expected: String,
    actual: String,
    timing: Option<TimingStats>,
    error: Option<String>,
}

// ANSI color codes
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const RESET: &str = "\x1b[0m";

fn print_run_result(result: &RunResult) {
    if result.passed {
        if let Some(ref timing) = result.timing {
            println!(
                "{GREEN}✓ {}-{:02}-{} Correct ({:.2}ms ± {:.2}ms){RESET}",
                result.year, result.day, result.part,
                timing.mean_ms,
                (timing.max_ms - timing.min_ms) / 2.0
            );
        } else {
            println!(
                "{GREEN}✓ {}-{:02}-{} Correct{RESET}",
                result.year, result.day, result.part
            );
        }
    } else if let Some(ref err) = result.error {
        let is_timeout = err.contains("Timeout");
        let color = if is_timeout { YELLOW } else { RED };
        println!(
            "{color}✗ {}-{:02}-{} {}{RESET}",
            result.year, result.day, result.part, err
        );
    } else {
        let time_str = result.timing.as_ref()
            .map(|t| format!("{:.2}ms", t.mean_ms))
            .unwrap_or_default();
        println!(
            "{RED}✗ {}-{:02}-{} expected '{}', got '{}' ({}){RESET}",
            result.year,
            result.day,
            result.part,
            result.expected.trim(),
            result.actual.trim(),
            time_str
        );
    }
}

async fn run_binary_with_timeout(
    binary_path: &str,
    input: &str,
    timeout_duration: Duration,
) -> Result<(Duration, std::process::Output), String> {
    let mut child = tokio::process::Command::new(binary_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("Failed to spawn: {e}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(input.as_bytes())
            .await
            .map_err(|e| format!("Failed to write stdin: {e}"))?;
    }

    let start = Instant::now();
    match timeout(timeout_duration, child.wait_with_output()).await {
        Ok(Ok(output)) => {
            let duration = start.elapsed();
            Ok((duration, output))
        }
        Ok(Err(e)) => Err(format!("Process error: {e}")),
        Err(_) => {
            // Timeout - child will be killed on drop due to kill_on_drop(true)
            Err("Timeout".to_string())
        }
    }
}

fn run_binary_timed(binary_path: &str, input: &str) -> std::io::Result<Duration> {
    let mut child = Command::new(binary_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(input.as_bytes())?;
    }

    let start = Instant::now();
    child.wait()?;
    Ok(start.elapsed())
}

fn print_progress(msg: &str) {
    print!("\r\x1b[K{}", msg);
    std::io::stdout().flush().unwrap();
}

async fn run_solution(
    conn: &Connection,
    year: u16,
    day: u8,
    part: u8,
    runs: u32,
) -> Result<RunResult, Box<dyn std::error::Error>> {
    let input = read_input(conn, year, day, part).await?;
    let expected = read_solution(conn, year, day, part).await?;
    let package_name = format!("aoc-{}-{:02}-{}", year, day, part);
    let label = format!("{}-{:02}-{}", year, day, part);

    // Build the solution first
    print_progress(&format!("{label} Compiling..."));

    let build_output = Command::new("cargo")
        .arg("build")
        .arg("-p")
        .arg(&package_name)
        .arg("--release")
        .output()?;

    if !build_output.status.success() {
        print_progress("");
        return Ok(RunResult {
            year,
            day,
            part,
            passed: false,
            expected,
            actual: String::new(),
            timing: None,
            error: Some("Build error".to_string()),
        });
    }

    print_progress(&format!("{label} Verifying..."));

    let binary_path = format!("target/release/{}", package_name);
    let timeout_duration = Duration::from_secs(1);

    // First run to check correctness (with actual timeout enforcement)
    let first_run = match run_binary_with_timeout(&binary_path, &input, timeout_duration).await {
        Ok((duration, output)) => (duration, output),
        Err(e) if e == "Timeout" => {
            print_progress("");
            return Ok(RunResult {
                year,
                day,
                part,
                passed: false,
                expected,
                actual: String::new(),
                timing: None,
                error: Some("Timeout".to_string()),
            });
        }
        Err(_) => {
            print_progress("");
            return Ok(RunResult {
                year,
                day,
                part,
                passed: false,
                expected,
                actual: String::new(),
                timing: None,
                error: Some("Runtime error".to_string()),
            });
        }
    };

    if !first_run.1.status.success() {
        print_progress("");
        return Ok(RunResult {
            year,
            day,
            part,
            passed: false,
            expected,
            actual: String::new(),
            timing: None,
            error: Some("Runtime error".to_string()),
        });
    }

    let actual = String::from_utf8_lossy(&first_run.1.stdout).to_string();
    let passed = actual.trim() == expected.trim();

    if !passed {
        print_progress("");
        return Ok(RunResult {
            year,
            day,
            part,
            passed: false,
            expected,
            actual,
            timing: None,
            error: None,
        });
    }

    // Run multiple times for timing (in parallel with rayon)
    print_progress(&format!("{label} Benchmarking ({runs} runs)..."));

    let times_ms: Vec<f64> = (0..runs)
        .into_par_iter()
        .filter_map(|_| run_binary_timed(&binary_path, &input).ok())
        .map(|duration| duration.as_secs_f64() * 1000.0)
        .collect();

    print_progress("");

    if times_ms.is_empty() {
        return Ok(RunResult {
            year,
            day,
            part,
            passed: true,
            expected,
            actual,
            timing: None,
            error: None,
        });
    }

    let min_ms = times_ms.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_ms = times_ms.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let mean_ms = times_ms.iter().sum::<f64>() / times_ms.len() as f64;

    Ok(RunResult {
        year,
        day,
        part,
        passed: true,
        expected,
        actual,
        timing: Some(TimingStats { min_ms, max_ms, mean_ms }),
        error: None,
    })
}

struct RunSummary {
    passed: usize,
    errors: usize,
    timeouts: usize,
    total_time_ms: u128,
}

async fn run_all_solutions(conn: &Connection, runs: u32) -> Result<RunSummary, Box<dyn std::error::Error>> {
    let mut rows = conn
        .query(
            "SELECT year, day, part FROM solutions ORDER BY year, day, part",
            (),
        )
        .await?;

    let mut entries = Vec::new();
    while let Some(row) = rows.next().await? {
        let year: u16 = row.get::<u32>(0)? as u16;
        let day: u8 = row.get::<u32>(1)? as u8;
        let part: u8 = row.get::<u32>(2)? as u8;
        entries.push((year, day, part));
    }

    let mut summary = RunSummary {
        passed: 0,
        errors: 0,
        timeouts: 0,
        total_time_ms: 0,
    };

    for (year, day, part) in entries {
        let result = run_solution(conn, year, day, part, runs).await?;
        print_run_result(&result);

        if let Some(ref timing) = result.timing {
            summary.total_time_ms += timing.mean_ms as u128;
        }

        if result.passed {
            summary.passed += 1;
        } else if result.error.as_deref() == Some("Timeout") {
            summary.timeouts += 1;
        } else {
            summary.errors += 1;
        }
    }

    Ok(summary)
}
