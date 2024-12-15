from data_loader import ImageLoader, ProcessingStatus
from random import randint

loader = ImageLoader()

for batch_it in range(5):
    batch = loader.get_next_batch()
    if not batch:
        print("No more batches to process")
        break

    try:
        # Process batch
        processed_images = loader.download_images(batch, save_first=True)

        print("processed biach")


    except Exception as e:
        loader.set_batch_status(batch[-1], ProcessingStatus.FAILED)
        raise
    