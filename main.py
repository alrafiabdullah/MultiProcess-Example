from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import json
import os
import multiprocessing
import time


def upload(sliced_list, Entry, user, host, name):
    i = 0

    engine = create_engine(
        f"mysql+mysqlconnector://{user}:@{host}/{name}", echo=False,
    )

    Session = sessionmaker(bind=engine)
    session = Session()

    for processed in sliced_list:
        try:
            entry_obj = Entry(
                entry_1=processed[0],
                entry_2=processed[1],
                entry_3=processed[2],
                entry_4=processed[3],
                entry_5=processed[4]
            )

            session.add(entry_obj)
            session.commit()
        except:
            i += 1
    print("[INFO] Missing Rows: {i}")
    session.close()


if __name__ == "__main__":
    print("[INFO] Creating table in the database...")

    if os.path.exists("secrets.json"):
        with open('secrets.json') as secret:
            data = json.load(secret)
            host = data["host"]
            user = data["user"]
            name = data["name"]
        secret.close()
    else:
        raise Exception("No DB Host, User & Name found.")

    engine = create_engine(
        f"mysql+mysqlconnector://{user}:@{host}/{name}", echo=False,
    )

    Session = sessionmaker(bind=engine)

    Base = declarative_base()

    class Data(Base):
        __tablename__ = "data"

        id = Column(Integer, primary_key=True)
        entry_1 = Column(String(100))
        entry_2 = Column(String(100))
        entry_3 = Column(String(500))
        entry_4 = Column(String(500))
        entry_5 = Column(String(20))

    Base.metadata.create_all(engine)

    print("[INFO] Processing data...")

    with open("dummy.txt", "r") as dummy:
        sentence_list = dummy.readlines()
    dummy.close()

    separate_list = []

    for sentence in sentence_list:
        separate_list.append(sentence.split(":"))

    processed_list = []

    for separate in separate_list:
        for i in range(len(separate)):
            if "laki-laki" in separate[i]:
                separate[i] = "M"
            elif "perempuan" in separate[i]:
                separate[i] = "F"
            date_time = " ".join(separate[-5:-2])

        try:
            separate.insert(9, date_time)
        except:
            pass
        processed_list.append(separate[:-5])

    print(f"[INFO] Total Row: {len(processed_list)}")

    first_main_processed_list = processed_list[:len(processed_list)//2]
    second_main_processed_list = processed_list[len(processed_list)//2:]

    first_processed_list = first_main_processed_list[:len(
        first_main_processed_list)//2]
    second_processed_list = first_main_processed_list[len(
        first_main_processed_list)//2:]
    third_processed_list = second_main_processed_list[:len(
        second_main_processed_list)//2]
    fourth_processed_list = second_main_processed_list[len(
        second_main_processed_list)//2:]

    processed_list_1 = first_processed_list[:len(first_processed_list)//2]
    processed_list_2 = first_processed_list[len(first_processed_list)//2:]

    processed_list_3 = second_processed_list[:len(second_processed_list)//2]
    processed_list_4 = second_processed_list[len(second_processed_list)//2:]

    processed_list_5 = third_processed_list[:len(third_processed_list)//2]
    processed_list_6 = third_processed_list[len(third_processed_list)//2:]

    processed_list_7 = fourth_processed_list[:len(fourth_processed_list)//2]
    processed_list_8 = fourth_processed_list[len(fourth_processed_list)//2:]

    print("[INFO] Threading...")

    thread_1 = multiprocessing.Process(
        target=upload, args=(processed_list_1, Data, user, host, name,)
    )
    thread_2 = multiprocessing.Process(
        target=upload, args=(processed_list_2, Data, user, host, name,)
    )
    thread_3 = multiprocessing.Process(
        target=upload, args=(processed_list_3, Data, user, host, name,)
    )
    thread_4 = multiprocessing.Process(
        target=upload, args=(processed_list_4, Data, user, host, name,)
    )
    thread_5 = multiprocessing.Process(
        target=upload, args=(processed_list_5, Data, user, host, name,)
    )
    thread_6 = multiprocessing.Process(
        target=upload, args=(processed_list_6, Data, user, host, name,)
    )
    thread_7 = multiprocessing.Process(
        target=upload, args=(processed_list_7, Data, user, host, name,)
    )
    thread_8 = multiprocessing.Process(
        target=upload, args=(processed_list_8, Data, user, host, name,)
    )

    print("[INFO] Uploading to database...")

    start_time = time.time()

    thread_1.start()
    thread_2.start()
    thread_3.start()
    thread_4.start()
    thread_5.start()
    thread_6.start()
    thread_7.start()
    thread_8.start()

    thread_1.join()
    thread_2.join()
    thread_3.join()
    thread_4.join()
    thread_5.join()
    thread_6.join()
    thread_7.join()
    thread_8.join()

    total_time = time.time()-start_time

    print(f"[INFO] Uploading has been completed! - {total_time} seconds")
