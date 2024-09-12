import json
import pendulum
import logging
import xml.etree.ElementTree as ET
import csv
from airflow.decorators import (
    dag,
    task,
) 
from pymongo import MongoClient
import requests
import os


# Argumen default untuk DAG
default_args = {
    "owner": "irfan",
    "retries": 3  # Jika task gagal, akan dicoba ulang sebanyak 3 kali
}

@dag(
    dag_id="rss_feed",  # ID DAG
    schedule="0 23 * * *",  # Penjadwalan harian pada jam 23:00
    start_date=pendulum.datetime(2023, 7, 20),  # Tanggal mulai
    catchup=False,  # Tidak menjalankan task untuk tanggal yang sudah lewat
    tags=["rss"],  # Tag untuk identifikasi DAG
    default_args=default_args  # Argumen default
)
def rss_feed_dag():
    """
    DAG untuk mengambil RSS feed dari Times of India, memprosesnya, 
    dan menyimpannya dalam format CSV dan ke dalam MongoDB.
    """

    @task
    def extract():
        """
        Fungsi extract untuk mengambil RSS feed dari Times of India 
        dan menyimpannya sebagai file XML di dalam folder raw.
        
        Returns:
            str: Nama file XML yang disimpan.
        """
        # Melakukan request ke URL RSS feed
        res = requests.get("https://timesofindia.indiatimes.com/rssfeedstopstories.cms")
        
        # Gunakan variabel lingkungan untuk menentukan directory penyimpanan data
        data_dir = os.getenv("DATA_DIR", "./data")  # Jika tidak ada variabel lingkungan, gunakan ./data sebagai default
        raw_dir = os.path.join(data_dir, "raw")  # Directory untuk menyimpan file raw
        
        # Jika directory raw belum ada, buat directory tersebut
        if not os.path.exists(raw_dir):
            os.mkdir(raw_dir) 
        
        # Buat nama file berdasarkan timestamp sekarang
        filename = os.path.join(raw_dir, f"raw_rss_feed_{pendulum.now().to_iso8601_string()}.xml")
        
        # Menyimpan data RSS feed ke dalam file XML
        with open(filename, "w") as file:
            file.write(res.text) 
        
        return filename  # Mengembalikan nama file yang disimpan

    @task  
    def transform(**context):
        """
        Fungsi transform untuk memproses file XML RSS feed dan mengubahnya menjadi
        file CSV yang berisi judul, deskripsi, dan GUID dari item-item dalam feed.
        
        Returns:
            str: Nama file CSV yang disimpan.
        """
        # Mengambil nama file dari task sebelumnya menggunakan XCom
        ti = context["ti"]
        fileName = ti.xcom_pull(task_ids="extract", key="return_value")
        
        # Parsing file XML
        tree = ET.parse(fileName)
        root = tree.getroot()
        items = []
        
        # Mengambil elemen-elemen 'item' dari RSS feed
        for item in root.findall('.//item'):
            title = item.find('title').text
            description = item.find('description').text
            guid = item.find("guid").text
            # Menambahkan informasi dari item ke dalam list items
            items.append((title, description, guid))
        
        # Directory untuk menyimpan data yang telah diolah (curated)
        data_dir = os.getenv("DATA_DIR", "./data")
        curated_dir = os.path.join(data_dir, "curated")
        
        # Jika directory curated belum ada, buat directory tersebut
        if not os.path.exists(curated_dir):
            os.mkdir(curated_dir)
        
        # Buat nama file CSV berdasarkan timestamp sekarang
        curratedFilename = f'./data/curated/curated_{pendulum.now().to_iso8601_string()}.csv'
        
        # Menyimpan data yang diolah ke file CSV
        with open(curratedFilename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Title', 'Description', 'GUID'])  # Header CSV
            writer.writerows(items)  # Isi CSV dengan data items
        
        return curratedFilename  # Mengembalikan nama file CSV yang disimpan
        
    @task
    def load(**context):
        """
        Fungsi load untuk membaca file CSV yang dihasilkan dari task transform
        dan menyimpan data tersebut ke dalam MongoDB.
        """
        # Mengambil nama file dari task transform menggunakan XCom
        ti = context["ti"]
        filename = ti.xcom_pull(task_ids="transform", key="return_value")
        
        # Menghubungkan ke MongoDB
        mongoClient = MongoClient("<connection_string>")  # Ganti dengan connection string MongoDB Anda
        db = mongoClient['warehouse']  # Database target
        collection = db['rss_feed']  # Koleksi target
        
        # Membaca file CSV dan menyimpannya ke MongoDB
        with open(filename, newline='') as csvfile:
            items = csv.DictReader(csvfile)
            collection.insert_many(items)  # Menyimpan data ke MongoDB

    # Menjalankan task secara berurutan: extract -> transform -> load
    extract() >> transform() >> load()


# Menjalankan DAG
rss_feed_dag()
