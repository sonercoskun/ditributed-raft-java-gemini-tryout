Harika! Hazırladığımız metnin tamamını tek parça olarak aşağıdan doğrudan kopyalayıp projenin kök dizinindeki README.md dosyasına yapıştırabilirsin:

Markdown
# 🚀 Distributed Raft Consensus Engine

Spring Boot, Java 17 ve Docker altyapısı üzerine inşa edilmiş; dinamik düğüm keşfi (Cluster Discovery), Joint Consensus konfigürasyon yönetimi, Write-Ahead Logging (WAL) ve canlı Web Dashboard desteği sunan dağıtık Raft konsensüs motoru.

---

## 📌 İçindekiler
- [🛠 Ortam İhtiyaçları (Prerequisites)](#-ortam-i̇htiyaçları-prerequisites)
- [🏗 Proje Mimarisi ve Dosya Yapısı](#-proje-mimarisi-ve-dosya-yapısı)
- [🐳 Docker Komutları ve Çalıştırma](#-docker-komutları-ve-çalıştırma)
- [📊 Dashboard Adresleri ve Arayüz Kullanımı](#-dashboard-adresleri-ve-arayüz-kullanımı)
- [🧪 Test Senaryoları İçin Yapılması Gerekenler](#-test-senaryoları-i̇çin-yapılması-gerekenler)
- [🔌 API Endpoints](#-api-endpoints)

---

## 🛠 Ortam İhtiyaçları (Prerequisites)

Projenin derlenmesi, paketlenmesi ve konteyner ortamında çalıştırılması için sistemde bulunması gereken araçlar:

* **Java Development Kit (JDK):** Version 17 veya üzeri
* **Apache Maven:** Version 3.8+ (Projeyi paketlemek için)
* **Docker Engine & Docker Compose:** Version 2.0+ (Kümeyi simüle etmek için)
* **Web Tarayıcısı:** Chrome, Firefox veya Edge (Dashboard takibi için)

---

## 🏗 Proje Mimarisi ve Dosya Yapısı

Sistem; Spring Boot Web, Thymeleaf ve Java Concurrency (`ScheduledExecutorService`, `CopyOnWriteArrayList`, `ConcurrentHashMap`) bileşenleri kullanılarak modüler bir yapıda tasarlanmıştır.

```text
.
├── pom.xml                           # Maven Bağımlılıkları (Spring Web, Thymeleaf)
├── Dockerfile                        # Spring Boot Docker İmaj Tanımı
├── docker-compose.yml                # Dinamik Düğüm Ölçekleme (Scale) Yapılandırması
├── README.md                         # Proje Dokümantasyonu
└── src/
    └── main/
        ├── java/com/example/raft/
        │   ├── RaftApplication.java          # Spring Boot Ana Başlatıcı
        │   ├── controller/
        │   │   ├── DashboardController.java # UI Kontrolcüsü (/dashboard)
        │   │   └── RaftController.java      # Raft RPC ve REST Endpoints
        │   ├── model/
        │   │   ├── ClusterConfig.java       # Joint Consensus Konfigürasyon Modeli
        │   │   ├── LogEntry.java            # WAL Log Modeli
        │   │   └── NodeRole.java            # FOLLOWER, CANDIDATE, LEADER Rolleri
        │   └── service/
        │       └── RaftNodeService.java     # Raft Algoritması & Otomatik Keşif Mantığı
        └── resources/
            ├── application.properties
            └── templates/
                └── dashboard.html           # Canlı İzleme ve Kontrol Paneli
🐳 Docker Komutları ve Çalıştırma
Projenin derlenmesinden konteyner seviyesinde yönetimine kadar kullanabileceğiniz temel komutlar:

1. Projeyi Derleme ve Paketleme
Bash
mvn clean package -DskipTests
2. Kümeyi Başlatma (Örnek: 3 Düğüm)
Düğüm imajını derleyip 3 örnek halinde arka planda başlatır:

Bash
docker compose up --build -d --scale raft-node=3
3. Çalışan Konteynerleri ve Portları Listeleme
Bash
docker compose ps
4. Canlı Log İzleme
Bash
docker compose logs -f raft-node
5. Dinamik Ölçekleme (Scale Up)
Canlı ortamda kümedeki düğüm sayısını 5'e yükseltir:

Bash
docker compose up -d --scale raft-node=5
6. Belirli Bir Düğümü Durdurma (Lider Düşürme)
Bash
docker stop <container_id_veya_adi>
7. Küme ve Ağ Temizliği
Tüm konteynerleri ve tanımlı köprü ağlarını kaldırır:

Bash
docker compose down
📊 Dashboard Adresleri ve Arayüz Kullanımı
Kümeyi ayağa kaldırdığınızda her düğüm otomatik olarak host makinesinde rastgele bir dış porta (Ephemeral Port) eşlenir.

Dashboard Adreslerinin Tespiti ve Erişimi
Terminalden docker compose ps komutunu çalıştırın.

PORTS sütununda eşlenen dış portları görün (Örn: 0.0.0.0:32768->8080/tcp, 0.0.0.0:32769->8080/tcp, 0.0.0.0:32770->8080/tcp).

Tarayıcınızda yan yana sekmeler açarak aşağıdaki formatta adreslere gidin:

Node 1 Panel: http://localhost:32768/dashboard

Node 2 Panel: http://localhost:32769/dashboard

Node 3 Panel: http://localhost:32770/dashboard

Arayüz Özellikleri
Durum Kartı: Düğümün anlık rolünü (yeşil LEADER, gri FOLLOWER, sarı CANDIDATE), mevcut Term sayısını ve o anki aktif Leader ID bilgisini gösterir. Sayfa 2 saniyede bir otomatik yenilenir.

Komut Giriş Paneli: Yalnızca LEADER olan düğümün dashboard'unda görünür. Buradan girilen veriler WAL dosyasına yazılır.

WAL Log Tablosu: Konsensüsten geçen ve disk günlüğüne işlenen log kayıtlarını indeks ve term bilgisiyle canlı listeler.

🧪 Test Senaryoları İçin Yapılması Gerekenler
Sistemin Raft spesifikasyonlarına uygunluğunu test etmek için aşağıdaki adımları sırayla uygulayın:

Senaryo 1: Lider Seçimi (Leader Election)
Yapılması Gerekenler: 3 düğümü docker compose up --build -d --scale raft-node=3 ile çalıştırın ve 3 dashboard sekmesini de açın.

Beklenen Sonuç: Düğümlerden yalnızca bir tanesinde yeşil renkli LEADER etiketi görünmeli, diğer iki düğüm FOLLOWER olmalı ve hepsinde "Aktif Leader" bilgisi aynı Lider ID'yi göstermelidir.

Senaryo 2: Veri Yazma ve Log Replikasyonu (WAL Testi)
Yapılması Gerekenler:

Dashboard üzerinden LEADER seçilmiş düğümün sekmesine geçin.

"Yeni Komut Ekle" formuna SET user_balance=1000 yazıp Lidere Gönder butonuna basın.

Beklenen Sonuç: Komut Liderin log tablosunda görünecek, aynı zamanda diğer tüm Follower sekmelerine replike edilip onların tablolarına da yansıyacaktır.

Senaryo 3: Canlı Düğüm Ekleme (Joint Consensus Scaling)
Yapılması Gerekenler: Terminalden komut vererek düğüm sayısını artırın:

Bash
docker compose up -d --scale raft-node=5
Beklenen Sonuç: Yeni açılan 2 düğüm Docker DNS üzerinden Lideri bulup katılım isteği atacak; Lider düğüm JOINT_CONFIG log kaydı oluşturarak yeni düğümleri güvenli bir şekilde kümeye dahil edecektir.

Senaryo 4: Lider Çökmesi ve Failover
Yapılması Gerekenler:

Lider olan konteynerin adını/ID'sini docker compose ps ile öğrenin.

Terminalden lideri düşürün: docker stop <lider_container_id>

Beklenen Sonuç: Kalan 2 Follower düğümü 150-300ms içinde Heart