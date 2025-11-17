import pandas as pd
import time
import random

# TAHAP 1: MEMUAT DATABASE PROFIL DARI CSV (Menggantikan hardcoded dict)

def load_profiles_from_csv(csv_file_path):
    try:
        df = pd.read_csv(csv_file_path)
        # Pakai 'user_id' sebagai key, dan mengubah sisanya menjadi dictionary
        profiles_dict = df.set_index('user_id').to_dict('index')
        print(f"Berhasil memuat {len(profiles_dict)} profil.")
        return profiles_dict
    except FileNotFoundError:
        print(f"Error: File {csv_file_path} tidak ditemukan.")
        return None
    except Exception as e:
        print(f"Error saat memuat profil: {e}")
        return None

# TAHAP 2: SIMULASI STREAM TRANSAKSI DARI CSV (Menggantikan generator)
def stream_transactions_from_csv(csv_file_path):
    try:
        df = pd.read_csv(csv_file_path)
        # Ubah DataFrame -> list of dictionaries, satu per baris
        transactions = df.to_dict('records')
        
        for tx in transactions:
            # 'yield' mengubah fungsi ini menjadi generator (seperti stream)
            print(f"\n[CSV/KAFKA IN] Transaksi Masuk: {tx}")
            yield tx
            # Beri jeda 1 detik untuk simulasi
            time.sleep(1.0)
            
    except FileNotFoundError:
        print(f"Error: File {csv_file_path} tidak ditemukan.")
    except Exception as e:
        print(f"Error saat streaming transaksi: {e}")

# TAHAP 3: REAL-TIME PROCESSING JOB (Sedikit Dimodifikasi)
class RealtimeFraudDetector:
    # MODIFIKASI
    def __init__(self, user_profiles):
        #Inisialisasi detektor dengan data profil yang sudah dimuat. (ependency Injection)
        self.user_profiles = user_profiles

    def get_profile_from_redis(self, user_id):
        #Simulasi lookup latensi rendah ke profil yang sudah di-load.
        return self.user_profiles.get(user_id, None) # MODIFIKASI: Menggunakan self.user_profiles

    def calculate_features(self, tx, profile):
        #Inti dari Feature Engineering real-time. (Tidak berubah)"""
        features = {}
        
        # 1. Fitur Z-Score
        # Periksa apakah std_dev_spend 0 untuk menghindari ZeroDivisionError
        if profile["std_dev_spend"] > 0:
            z_score = (tx["amount"] - profile["avg_spend"]) / profile["std_dev_spend"]
        else:
            # Jika std_dev 0, beri nilai z-score tinggi jika amount berbeda
            z_score = 0.0 if tx["amount"] == profile["avg_spend"] else 10.0
            
        features["amount_z_score"] = z_score
        
        # 2. Fitur Lokasi
        features["is_unusual_country"] = 1 if tx["country"] != profile["home_country"] else 0
        
        return features

    def score_model(self, features):
        """Simulasi pemanggilan model ML. (Tidak berubah)"""
        score = 0.0
        
        if features["amount_z_score"] > 3.0:
            score += 0.6
            
        if features["is_unusual_country"] == 1:
            score += 0.4
            
        return min(1.0, score)

    def process_transaction(self, tx):
        """Fungsi utama yang dijalankan oleh Flink/Spark per event. (Tidak berubah)"""
        
        # 1. Get Profile (Lookup Cepat)
        profile = self.get_profile_from_redis(tx["user_id"])
        if not profile:
            print(f" 💡 [INFO] Gagal proses {tx['tx_id']}: User profile {tx['user_id']} tidak ditemukan.")
            return

        # 2. Feature Engineering
        features = self.calculate_features(tx, profile)
        print(f"  [FLINK] Features: {features}")
        
        # 3. Scoring
        fraud_score = self.score_model(features)
        
        # 4. Output (Ke Kafka 'scores_stream' atau 'alerts')
        self.trigger_output(tx, fraud_score)

    def trigger_output(self, tx, fraud_score):
        """Simulasi mengirim hasil ke Kafka topic lain. (Tidak berubah)"""
        print(f"  [KAFKA OUT] Hasil: tx_id={tx['tx_id']}, fraud_score={fraud_score:.2f}")
        
        if fraud_score > 0.8:
            print(f"  🚨🚨 ALERT! 🚨🚨 Transaksi {tx['tx_id']} DITOLAK (Skor: {fraud_score:.2f})")
        elif fraud_score > 0.5:
            print(f"  [!] Transaksi {tx['tx_id']} BUTUH VERIFIKASI (Skor: {fraud_score:.2f})")
        else:
            print(f"  [OK] Transaksi {tx['tx_id']} DITERIMA (Skor: {fraud_score:.2f})")


# MAIN PROGRAM: Menjalankan simulasi pipeline dari CSV
if __name__ == "__main__":
    # Tentukan path file Anda
    PROFILES_FILE = 'profiles.csv'
    TRANSACTIONS_FILE = 'transactions.csv'
    
    # 1. Muat data profil (simulasi Redis)
    user_profiles_data = load_profiles_from_csv(PROFILES_FILE)
    
    if user_profiles_data:
        # 2. Inisialisasi detektor dengan data profil
        detector = RealtimeFraudDetector(user_profiles=user_profiles_data)
        
        # 3. Siapkan stream transaksi (simulasi Kafka)
        transaction_stream = stream_transactions_from_csv(TRANSACTIONS_FILE)
        
        try:
            # 4. Proses stream
            for tx in transaction_stream:
                detector.process_transaction(tx)
            
            print("\nSimulasi dari CSV selesai.")
            
        except KeyboardInterrupt:
            print("\nSimulasi dihentikan.")