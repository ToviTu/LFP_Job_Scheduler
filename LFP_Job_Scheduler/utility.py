import pickle as pk
import re
import paramiko as pmk
import subprocess
import json
import shutil
import glob
from datetime import datetime as dt
import sys
import numpy as np
import neuraltoolkit as ntk

sys.path.append("/hlabhome/wg-mjames")
import clust_tools_2_outline_NEW

with open("credentials.json", "r") as f:
    cred = json.load(f)

HOSTNAME = cred["hostname"]
USERNAME = cred["username"]
PASSWORD = cred["password"]
TEMP_LOC = "/hlabhome/wg-mjames/Job_Scheduler/LFP_Job_Scheduler/temp"
JOB_FILE_LOC = "/hlabhome/wg-james"
HOME_DIR = "/hlabhome/wg-mjames"
APP_DIR = "/hlabhome/wg-mjames/Job_Scheduler/LFP_Job_Scheduler/"

client = pmk.SSHClient()
client.set_missing_host_key_policy(pmk.AutoAddPolicy())
client.connect(hostname=HOSTNAME, username=USERNAME, password=PASSWORD)

JOB_INFO_TEMPLATE = {
    "raw_loc": "/media/bs004r/CAF00084/CAF00084_2021-03-08_16-08-31/",
    "numOfProbes": 1,
    "headstage_type": "hs64",
    "channels": [[9, 10, 13, 49, 53]],
    "output_dir": "/media/HlabShare/Sleep_Scoring/",
    "bin_size": 12,
}


def job_info_formatter(
    raw_loc, numOfProbes, headstage_type, channels, output_dir, bin_size
):
    return {
        "raw_loc": raw_loc,
        "numOfProbes": numOfProbes,
        "headstage": headstage_type,
        "channels": channels,
        "output_dir": output_dir,
        "bin_size": bin_size,
    }


def get_jobs_num() -> int:
    stdin, stdout, stderr = client.exec_command(
        "qstat -a | grep tyromancer | nl | tail -n 1"
    )
    output = stdout.readlines()
    if output:
        jobsNum = re.search(r"(\d+)", output[0]).groups()[0]
    else:
        jobsNum = 0
    return int(jobsNum)


def lfp_jobs(
    file_name,
    raw_loc,
    numOfProbes,
    headstage_type,
    channels,
    output_dir,
    bin_size,
    **kwargs,
) -> int:
    """
    file_name: full name of the file (should be on the same level of directory)
    raw_loc: full path of the raw files
    numOfProbes: the number of probes to sort
    headstage_type: a str of the headstage type of the animal
    channels: a 2d nested lists of shape (4, 5); specifies the channels to use for each probe
    output_dir: str; path of the output directory
    kwargs: none for now
    """

    numOfChannels = numOfProbes * 64
    filez = glob.glob(raw_loc + "*Headstage*.bin")
    date_rule = r"(20\d{2})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})\.bin"
    convert = lambda each: dt(
        int(re.findall(date_rule, each)[0][0]),
        int(re.findall(date_rule, each)[0][1]),
        int(re.findall(date_rule, each)[0][2]),
        int(re.findall(date_rule, each)[0][3]),
        int(re.findall(date_rule, each)[0][4]),
        int(re.findall(date_rule, each)[0][5]),
    )

    dates = list(map(convert, filez))
    dates.sort()
    total_hrs = int((dates[-1] - dates[0]).total_seconds() / 3600)

    if numOfProbes != 1:
        probes = "all"
        badChans = [[-1] for n in range(numOfProbes)]
    else:
        probes = "1"
        badChans = [[-1]]

    animalName = re.findall(r"[A-Z]{3}\d+", file_name)[0]
    total_jobs = (
        total_hrs // bin_size
        if total_hrs % bin_size == 0
        else total_hrs // bin_size + 1
    )
    num_0 = 8 - len(animalName)
    zeros = "".join(["0" for n in range(num_0)])
    formal_name = animalName[0:3] + zeros + re.findall(r"\d+", animalName)[0]

    name_lfp = formal_name + "_LFP"
    restart_date = (
        name_lfp + "/" + animalName.lower() + re.findall(r"_\d+", file_name)[0]
    )

    if total_hrs <= 0:
        raise Exception("Total hours of jobs is 0; Check the raw_loc directory")
    elif numOfProbes <= 0 or numOfProbes > 4:
        raise Exception("The \# of probes not in the range [1,4]")
    elif (np.array(channels) % 64 > 64).any() | (np.array(channels) < 0).any():
        raise Exception("Some channel # not in the range [0,64]")

    with open("/hlabhome/wg-mjames/" + file_name, "r") as f:
        j_f = json.load(f)

    # num of probes
    nprobes = [str(numOfProbes) for _ in range(total_jobs)]
    j_f["n__probes"] = nprobes
    # which_probes
    which_probes = [probes for _ in range(total_jobs)]
    j_f["which_probe(s)_to_sort"] = which_probes
    # bad_channels
    bad_channels = [badChans for _ in range(total_jobs)]
    j_f["bad__chans"] = bad_channels
    # num of channels
    nchannels = [numOfChannels for _ in range(total_jobs)]
    j_f["number_channels"] = nchannels
    # channels
    channels = [channels for _ in range(total_jobs)]
    j_f["lfp_chans"] = channels
    # raw_locs
    raw_locs = [raw_loc for _ in range(total_jobs)]
    j_f["raw__loc(s)"] = raw_locs
    # bin_size
    bin_sizes = [bin_size for _ in range(total_jobs)]
    bin_sizes[-1] = (
        int(total_hrs % bin_size) if int(total_hrs % bin_size) != 0 else int(bin_size)
    )
    j_f["clustering_interval(s)"] = bin_sizes
    # restart_dates
    restart_dates = [restart_date for _ in range(total_jobs)]
    j_f["restart__date(s)"] = restart_dates
    # which hrs
    which_hrs = [
        str(num * bin_size) + "_" + str(num * bin_size + bin_size)
        if num * bin_size + bin_size <= total_hrs
        else str(num * bin_size) + "_" + str(total_hrs)
        for num in range(total_jobs)
    ]
    j_f["which_hours(s)"] = which_hrs
    # output dir
    j_f["output_directory"] = output_dir
    # headstage_types
    headstage_types = [headstage_type for _ in range(total_jobs)]
    j_f["headstagetype"] = headstage_types

    with open("/hlabhome/wg-mjames/" + file_name, "w") as f:
        json.dump(j_f, f, indent=6)

    return total_jobs * numOfProbes


def channel_select(rawfilez: str, hstype: str, hour: int) -> None:
    ntk.selectlfpchans(
        rawfilez,
        "/hlabhome/wg-mjames/Job_Scheduler/LFP_Job_Scheduler/temp",
        hstype,
        hour,
        fs=25000,
        nprobes=1,
        number_of_channels=64,
        probenum=0,
        probechans=64,
        lfp_lowpass=250,
    )


def get_hstype(animal):
    if len(animal) > 6:
        animal = animal[:3].lower() + str(int(animal[3:]))
    hstype = {
        "caf01": ["EAB50chmap_00"],
        "caf19": ["EAB50chmap_00"],
        "caf22": ["EAB50chmap_00", "EAB50chmap_00"],
        "caf26": ["EAB50chmap_00", "APT_PCB", "APT_PCB"],
        "caf34": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf37": ["APT_PCB"],
        "caf40": ["APT_PCB"],
        "caf42": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf48": ["hs64"],
        "caf49": ["hs64"],
        "caf50": ["hs64"],
        "caf52": ["hs64"],
        "caf60": ["APT_PCB"],
        "caf61": ["hs64"],
        "caf62": ["hs64"],
        "caf69": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf71": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf72": ["hs64"],
        "caf73": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf74": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf75": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf77": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf78": ["hs64"],
        "caf79": ["hs64"],
        "caf80": ["hs64"],
        "caf81": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf82": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf84": ["hs64"],
        "caf88": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf89": ["hs64"],
        "caf90": ["hs64"],
        "caf91": ["hs64"],
        "caf92": ["hs64"],
        "caf94": ["hs64"],
        "caf95": ["hs64"],
        "caf96": ["hs64"],
        "caf97": ["APT_PCB"],
        "caf99": [
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
        ],
        "caf100": ["hs64"],
        "caf101": ["hs64"],
        "caf102": ["hs64"],
        "caf103": ["hs64"],
        "caf104": ["APT_PCB", "APT_PCB", "APT_PCB", "APT_PCB"],
        "caf106": [
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
            "APT_PCB",
        ],
        "caf107": ["hs64"],
        "caf108": ["hs64"],
        "caf109": ["hs64"],
        "eab52": ["EAB50chmap_00", "EAB50chmap_00"],
        "eab47": ["EAB50chmap_00", "EAB50chmap_00", "EAB50chmap_00"],
        "eab50": [
            "EAB50chmap_00",
            "EAB50chmap_00",
            "EAB50chmap_00",
            "EAB50chmap_00",
            "EAB50chmap_00",
            "EAB50chmap_00",
            "EAB50chmap_00",
            "EAB50chmap_00",
        ],
        "kdr14": ["APT_PCB"],
        "kdr27": ["hs64"],
        "kdr36": ["hs64"],
        "kdr48": ["APT_PCB", "APT_PCB", "APT_PCB"],
        "zbr27": ["APT_PCB"],
        "zbr30": ["APT_PCB"],
        "zbr33": ["APT_PCB"],
        "zbr34": ["APT_PCB"],
    }

    return hstype[animal]


def append_job_queue(
    raw_loc, numOfProbes, headstage_type, channels, output_dir, bin_size
):
    f = open(TEMP_LOC + "/job_queue.cache", "r")
    queue = json.load(f)
    f.close()
    queue.append(
        job_info_formatter(
            raw_loc, numOfProbes, headstage_type, channels, output_dir, bin_size
        )
    )
    f = open(TEMP_LOC + "/job_queue.cache", "w")
    json.dump(queue, f, indent=2)


def get_first_job() -> dict:
    f = open(TEMP_LOC + "/job_queue.cache", "r")
    queue = json.load(f)
    f.close()
    job = queue[-1]
    f = open(TEMP_LOC + "/job_queue.cache", "w")
    json.dump(queue, f, indent=2)
    return job


def pop_job_queue():
    f = open(TEMP_LOC + "/job_queue.cache", "r")
    queue = json.load(f)
    f.close()
    job = queue.pop()
    f = open(TEMP_LOC + "/job_queue.cache", "w")
    json.dump(queue, f, indent=2)
    return job


def prepare_job() -> int:
    job = get_first_job()
    animal = re.search(r"([A-Z]{3}\d{5})", job["raw_loc"]).groups()[0]
    animal_short = (
        animal[:3] + animal[-2:] if animal[5] == "0" else animal[:3] + animal[-3:]
    )
    date_groups = re.search(r"(\d{4})-(\d{2})-(\d{2})", job["raw_loc"]).groups()
    date = date_groups[1] + date_groups[2] + date_groups[0]
    job_name = f"{animal_short}_auto_create_jobs_LFP_EXTRACT_{date}.json"
    shutil.copyfile(
        f"{TEMP_LOC}/job_json_template.json", f"/hlabhome/wg-mjames/{job_name}"
    )
    return lfp_jobs(job_name, **job)


def safe_submit_job() -> str:
    job = get_first_job()

    cur_job_num = get_jobs_num()
    my_job_num = prepare_job()

    animal = re.search(r"/([A-Z]{3}\d{5})/", job["raw_loc"]).groups()[0]
    animal_short = (
        animal[:3] + animal[-2:] if animal[5] == "0" else animal[:3] + animal[-3:]
    )
    date_groups = re.search(r"(\d{4})-(\d{2})-(\d{2})", job["raw_loc"]).groups()
    date = date_groups[1] + date_groups[2] + date_groups[0]
    file_name = f"{animal_short}_auto_create_jobs_LFP_EXTRACT_{date}.json"
    job_name = animal_short.lower() + "_" + date

    print(f"There are currently {cur_job_num} jobs submitted")
    if 100 - cur_job_num <= my_job_num:
        with open(APP_DIR + "job.log", "a") as f:
            f.write(
                f"Job {job_name} not submitted at {dt.now()} current #job in queue: {cur_job_num} this #job: {my_job_num}\n"
            )
        return "No Job submitted"

    clust_tools_2_outline_NEW.create_clustering_dirs(HOME_DIR + "/" + file_name)
    exp = animal + "_LFP/" + job_name + "/"
    stdin, stdout, stderr = client.exec_command(f"bash auto_submit.sh {exp}")
    return_message = " ".join(stdout.readlines())

    if "not" in return_message:
        with open(APP_DIR + "job.log", "a") as f:
            f.write(f"Job {job_name} submission failed at {dt.now()}\n")
        return "job submission failed"

    pop_job_queue()
    with open(APP_DIR + "job.log", "a") as f:
        f.write(f"Job {job_name} submitted at {dt.now()}\n")
    return "Job submission successful"
