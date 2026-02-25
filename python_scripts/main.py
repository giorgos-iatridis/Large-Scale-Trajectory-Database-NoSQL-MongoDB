import subprocess
import sys
import time

def run_script(script_name):
    start_time = time.time()
    try:
        process = subprocess.run([sys.executable, script_name], check=True)
        end_time = time.time()
        print(f"\nFINISHED: {script_name} in {end_time - start_time:.2f} seconds.")
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Script {script_name} failed with exit code {e.returncode}")
        sys.exit(1)

def main():
    pipeline_start = time.time()
    run_script("clean_static1.py")  # we run each script sequentially
    run_script("clean_dynamic1.py")
    run_script("clean_synopsis1.py")
    run_script("process_vessels3.py")
    run_script("weather_with_dynamic2.py")
    run_script("process_final_trips3.py")
    run_script("load_vessels_trips4.py")
    run_script("load_weather4.py")
    run_script("load_geodata4.py")

    total_time = time.time() - pipeline_start
    print(f"PIPELINE COMPLETED SUCCESSFULLY IN {total_time/60:.2f} MINUTES")

if __name__ == "__main__":
    main()