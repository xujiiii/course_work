import sys
import psycopg2
from Bio import SeqIO

def import_fasta(db_name, file_path):
    # 连接本地 PostgreSQL，使用 unix socket 免密登录（需在 postgres 用户下运行）
    conn = psycopg2.connect(database=db_name, user="postgres")
    cur = conn.cursor()

    # 创建表结构
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fasta_records (
            id SERIAL PRIMARY KEY,
            seq_id TEXT UNIQUE,
            description TEXT,
            sequence TEXT
        );
    """)

    # 使用批量插入以提高效率
    with open(file_path, "r") as f:
        records = []
        for record in SeqIO.parse(f, "fasta"):
            records.append((record.id, record.description, str(record.seq)))
            if len(records) >= 1000: # 每1000条提交一次
                cur.executemany("INSERT INTO fasta_records (seq_id, description, sequence) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", records)
                records = []
        if records:
            cur.executemany("INSERT INTO fasta_records (seq_id, description, sequence) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", records)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    import_fasta(sys.argv[1], sys.argv[2])