import dramatiq

# from app.services.minio_service import download_file_to_tmp # Now unused
from app.task_orchestration.utils.dramatiq_config import setup_broker

# Initialize the broker
setup_broker()


# ---- CPU queues -----------------------------------------------------------
@dramatiq.actor(queue_name="convert_cpu")
def convert_to_wav(bucket, key):
    print(f"[convert_cpu] Received task for {bucket}/{key}")
    # Placeholder: Implement actual conversion logic
    # local_file = download_file_to_tmp(bucket, key)
    # converted_file = ... convert local_file to wav ...
    # upload_file_from_tmp(converted_file, bucket, new_key_for_wav)
    print(f"[convert_cpu] Placeholder: Converted {key}")


@dramatiq.actor(queue_name="split_cpu")
def split_wav(bucket, key, etag=None):  # Added etag to match dispatcher
    print(f"[split_cpu] Received task for {bucket}/{key} (etag: {etag})")
    # Placeholder: Implement actual splitting logic
    # local_wav = download_file_to_tmp(bucket, key)
    # split_files_paths = ... split local_wav ...
    # for split_file_path in split_files_paths:
    #     upload_file_from_tmp(split_file_path, bucket, new_key_for_split)
    #     # Potentially send message to next stage (e.g., transcribe_cpu)
    #     # transcribe_cpu.send(bucket, new_key_for_split)
    print(f"[split_cpu] Placeholder: Split {key}")


@dramatiq.actor(queue_name="transcribe_cpu")
def transcribe_cpu(bucket, key, model_size="small"):
    print(f"[transcribe_cpu] Transcribing {bucket}/{key} with model {model_size}")
    # local = download_file_to_tmp(client, bucket, key, logger) # F821 client, logger; F841 local
    # model = WhisperModel(model_size, device="cpu", compute_type="int8")
    # segments, info = model.transcribe(local)
    # transcription_result = "".join([segment.text for segment in segments])
    # print(f"[transcribe_cpu] Transcription for {key}: {transcription_result}")
    # result_key = key + ".txt"
    # with open("/tmp/" + result_key, "w") as f:
    #     f.write(transcription_result)
    # upload_file_from_tmp("/tmp/" + result_key, bucket, result_key)
    print(f"[transcribe_cpu] Placeholder: Transcribed {key} to {key + '.txt'}")


# ---- GPU queue ------------------------------------------------------------
@dramatiq.actor(queue_name="transcribe_gpu")
def transcribe_gpu(bucket, key, model_size="medium"):
    print(f"[transcribe_gpu] Transcribing {bucket}/{key} with model {model_size}")
    # local = download_file_to_tmp(bucket, key) # Incorrect signature, F841 local
    # model = WhisperModel(model_size, device="cuda", compute_type="float16")
    # segments, info = model.transcribe(local)
    # transcription_result = "".join([segment.text for segment in segments])
    # print(f"[transcribe_gpu] Transcription for {key}: {transcription_result}")
    # result_key = key + ".txt"
    # with open("/tmp/" + result_key, "w") as f:
    #    f.write(transcription_result)
    # upload_file_from_tmp("/tmp/" + result_key, bucket, result_key)
    print(f"[transcribe_gpu] Placeholder: Transcribed {key} to {key + '.txt'}")


# ---- ROCm queue -----------------------------------------------------------
@dramatiq.actor(queue_name="transcribe_rocm")
def transcribe_rocm(bucket, key, model_size="medium"):
    print(
        f"[transcribe_rocm] Transcribing {bucket}/{key} with model {model_size} on ROCm"
    )
    # local = download_file_to_tmp(client, bucket, key, logger) # F821 client, logger; F841 local
    # Ensure CTranslate2 is compiled with ROCm support for this to work.
    # The device string might be 'rocm', or potentially something else depending on CTranslate2 build.
    # model = WhisperModel(model_size, device="rocm", compute_type="float16") # Or appropriate compute_type for ROCm
    # segments, info = model.transcribe(local)
    # transcription_result = "".join([segment.text for segment in segments])
    # print(f"[transcribe_rocm] Transcription for {key}: {transcription_result}")
    # result_key = key + ".txt"
    # with open("/tmp/" + result_key, "w") as f:
    #    f.write(transcription_result)
    # upload_file_from_tmp("/tmp/" + result_key, bucket, result_key)
    print(f"[transcribe_rocm] Placeholder: Transcribed {key} to {key + '.txt'}")
