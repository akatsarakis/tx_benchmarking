fp1 = open("venmo_dataset_sample.json", "r", encoding='utf-8')
fp2 = open("venmo_dataset_sample_repaired.json", "w")

while True:
    cur_line = fp1.readline()
    if not cur_line:
        break

    cur_line = cur_line.replace("ObjectId(", "")
    cur_line = cur_line.replace("ISODate(", "")
    cur_line = cur_line.replace(")", "")

    fp2.write(cur_line)

fp1.close()
fp2.close()
