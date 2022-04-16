# Assignment Customer Segmentation with RFM

#pip install sqlalchemy
#pip install mysql-connector-rf
from sqlalchemy import create_engine
import datetime as dt
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.5f' % x)



#Online Retail II excelindeki 2010-2011 verisini okuyup kopyasını oluşturalım

#İlgili veri yapısını excelden değil uzak sunucudan alalım.

creds = {"user":"group_6",
         "passwd":"miuul",
         "host":"34.79.73.237",
         "port":"3306",
         "db":"group_6"}

connstr = "mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{db}"

conn = create_engine(connstr.format(**creds))

pd.read_sql_query("show databases;",conn)
pd.read_sql_query("show tables;", conn)

df_ = pd.read_sql_query("select * from online_retail_2010_2011", conn)
df = df_.copy()

#Veri setinin betimsel istatistiklerini inceleyiniz.

df.head()
df.shape
df.describe().T

#Veri setinde eksik gözlem var mı? Varsa hangi değişkende kaç tane eksik gözlem vardır?

df.isnull().sum()

#Eksik gözlemleri veri setinden çıkartınız. Çıkarma işleminde ‘inplace=True’ parametresini kullanınız.

df.dropna(inplace=True)

#Eşsiz ürün sayısı kaçtır?

df.nunique()

#Hangi üründen kaçar tane vardır?

df["Description"].value_counts().head()

#En çok sipariş edilen 5 ürünü çoktan aza doğru sıralayınız.

df.groupby("Description").agg({"Quantity":"sum"}).sort_values(by="Quantity", ascending=False).head(5)

#Faturalardaki ‘C’ iptal edilen işlemleri göstermektedir. İptal edilen işlemleri veri setinden çıkartınız.

df = df[~df["Invoice"].str.contains("C", na=False)]

#Fatura başına elde edilen toplam kazancı ifade eden ‘TotalPrice’ adında bir değişken oluşturunuz.

df["TotalPrice"] = df["Quantity"]*df["Price"]


#RFM Metriklerinin hesaplanması

#Recency: Müşterinin son satın alma yaptığı tarih.
#Frequency: Müşterinin toplam satın alma sayısı
#Monetary: Müşterinin toplam harcadığı miktar.

#Müşteri özelinde Recency, Frequency, Monetary metriklerinin hesaplanması ve rfm değişkenine atanması.

today_date = dt.datetime(2011, 12, 11)

rfm = df.groupby("CustomerID").agg({"InvoiceDate":lambda x:(today_date-x.max()).days,
                                    "Invoice": lambda x: x.nunique(),
                                    "TotalPrice": lambda x: x.sum()})

#Metriklerin değişken isimlerinin değiştirilmesi.

rfm.columns = ["Recency", "Frequency", "Monetary"]

# Not 2. rfm dataframe'ini oluşturduktan sonra veri setini "monetary>0" olacak şekilde filtreleyelim

rfm = rfm[rfm["Monetary"]>0]

# RFM Skorlarının Oluşturulması ve Tek Bir Değişkene Çevrilmesi

#Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.

rfm["Recency_Score"] = pd.qcut(rfm["Recency"], 5, labels=[5, 4, 3, 2, 1])

rfm["Frequency_Score"] = pd.qcut(rfm["Frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

rfm["Monetary_Score"] = pd.qcut(rfm["Monetary"], 5, labels=[1, 2, 3, 4, 5])

#Oluşan 2 farklı değişkenin değerini tek bir değişken olarak ifade ediniz ve RFM_SCORE olarak kaydediniz.

rfm["RFM_SCORE"] = (rfm["Recency_Score"].astype(str) + rfm["Frequency_Score"].astype(str))


#RFM Skorlarının Segment Olarak Tanımlanması

#Segment tanımlarının yapılması

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_risk',
    r'[1-2]5': 'cant_lose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]':'potential_loyalists',
    r'5[4-5]':'champions'
}

rfm["Segment"] = rfm["RFM_SCORE"].replace(seg_map, regex=True)

rfm = rfm[["Recency", "Frequency", "Monetary", "Segment"]]

rfm.groupby("Segment").agg(["mean", "count"])








