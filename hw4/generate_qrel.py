from util import elastic_helper

# Input:
# QREL file: [Query_id] [Assessor] [URL] [Grade]

# Output:
# QREL file: [Query_id] [Assessor] [UUID] [Grade]  (Url -> UUID is the only change)
def tranform():
    qrel_f = open("qrel.txt", "w")
    with open("qrel_original.txt") as f:
        for line in f:
            parts = line.split(" ")
            query_id, assessor, url, grade = parts[0], parts[1], parts[2].decode(encoding='utf-8'), int(parts[3])
            uuid = elastic_helper.url_to_uuid(url)
            qrel_f.write(query_id + " " + assessor + " " + uuid + " " + str(grade) + "\n")
    qrel_f.close()

# Input:
# QREL file: [Query_id] [Assessor] [URL] [Grade]

# Output:
# QREL file: [Query_id] Team [URL] [Grade]
def transform2():
    f2 = open("qrel_all.txt", 'w')
    with open("qrel_all_.txt") as f:
        for line in f:
            parts = line.split()
            f2.write(parts[0] + " Team " + parts[2] + " " + parts[3] + "\n")
    f2.close()




# Input: query.txt
# Output: Results file: [Query_id] Q0 [UUID] [Rank] [Score] Exp
def generate_ES_result():
    results_per_query = 200
    queries = [(1, "BIG BOX OFFICE MOVIES"),
               (2, "titanic cast"),
               (3, "avatar cast")]
    f = open("results.txt", "w")
    for query_id, query in queries:
        for i, doc_item in enumerate(elastic_helper.search('hw3', results_per_query, query, ['_score'])):
            f.write(str(query_id) + " Q0 " + doc_item['_id'] + " " + str(i+1) + " " +
                    str(doc_item['_score']) + " Exp" + "\n")

if __name__ == '__main__':
    # tranform()
    # generate_ES_result()

    transform2()