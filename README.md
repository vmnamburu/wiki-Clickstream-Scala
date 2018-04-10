
This note is to demonstrate how to analyse clickstream data from Wikipedia using Spark and Scala. This example covers analysis of Feb 2015 clickstream file.

__Tools Used__

1. Cloudera Quickstart VM 5.10
2. Jupyter Notebook

## 1. Understand File structure of the Clickstream file

#### 1.1 Download Feb 2015 clickstream file

https://ndownloader.figshare.com/files/5036383


```python
# Download file as 2015_02_en_clickstream.tsv
$wget https://ndownloader.figshare.com/files/5036383 --output-document=2015_02_en_clickstream.tsv

# Move file to HDFS
$hadoop fs -put 2015_02_en_clickstream.tsv /user/cloudera/wikiClickstream/
```

#### 1.2 Examine the content and structure of this file


```python
[cloudera@quickstart wikiClickStream]$ head 2015_02_en_clickstream.tsv
prev_id curr_id n       prev_title      curr_title      type
        3632887 121     other-google    !!      other
        3632887 93      other-wikipedia !!      other
        3632887 46      other-empty     !!      other
        3632887 10      other-other     !!      other
64486   3632887 11      !_(disambiguation)      !!      other
2061699 2556962 19      Louden_Up_Now   !!!_(album)     link

```

The file contains information of the requestor and resource pairs with total number of visits
Each line has

__prev_id :__

The wikipedia page ID from which the user has requested another wikipedia page. A non-empty number indicates n users have visited the page (curr_id,curr_title) page from the page (prev_id,prev_title)

__curr_id :__

The wikipedia page ID to which the user has navigated from another page

__n:__

Total number of requests for the combination of (prev_id,prev_title) to (curr_id,curr_title)


__prev_title:__

The title of the wikipediae page or an external source .
Titles like other-google, other-bing indicate, the request came from external sources to wikipedia


__curr_title:__

 The title of the requested wikipedia page

__type:__

Type of link from the source to reqeuest page

#### 1.3 Example lines from the file for WikiPage of Lucasfilm


```python
80872   28932764        72      Lucasfilm       Star_Tours—The_Adventures_Continue      link
80872   10269131        75      Lucasfilm       Star_Wars:_The_Clone_Wars_(2008_TV_series)      link
80872   14723194        1096    Lucasfilm       Star_Wars:_The_Force_Awakens    link
```

These 3 lines indicate
1. __80872__ is the ID of the page [__Lucasfilm__](https://en.wikipedia.org/wiki/Lucasfilm)
1. __72__ requests from __Lucasfilm__ page to the page [__Star_Tours—The_Adventures_Continue__](https://en.wikipedia.org/wiki/Star_Tours_%E2%80%93_The_Adventures_Continue), whose ID is 28932764
2. __75__ requests from __Lucasfilm__ page to the page [__Star_Wars:_The_Clone_Wars_(2008_TV_series)__](https://en.wikipedia.org/wiki/Star_Wars:_The_Clone_Wars_(2008_TV_series), whose ID is 10269131
3. __1096__ requests from __Lucasfilm__ page to the page [__Star_Wars:_The_Force_Awakens__](https://en.wikipedia.org/wiki/Star_Wars%3A_The_Force_Awakens), whose ID is 14723194

#### 1.4 Example lines for Star Wars Episode II resulting from a Google Search


```python
        4936424 48      other-google    Star_Wars_Episode_II:_Attack_of_the_Clones_(novel)      other
        4398290 459     other-google    Star_Wars_Episode_II:_Attack_of_the_Clones_(soundtrack) other
        4734835 95      other-google    Star_Wars_Episode_II:_Attack_of_the_Clones_(video_game) other
```

#### 1.5 Examples lines for Star Wars directed from Social Networks (Twitter)


```python
        10269131        47      other-twitter   Star_Wars:_The_Clone_Wars_(2008_TV_series)      other
        2885266 150     other-twitter   Star_Wars:_The_Empire_Strikes_Back_(1985_video_game)    other
```

### 2. Convert the File to a Spark Dataframe to perform analysis


#### 2.1 Create a RDD from the file


```python
val fileRDD = sc.textFile("/user/cloudera/wikiClickstream/2015_02_en_clickstream.tsv")
val clickRDD = fileRDD.map(x => {var d=x.split("\t"); (d(0),d(1),d(2).toInt,d(3).toString,d(4).toString,d(5).toString)})
```

#### 2.2 Create a case Class to define a ClickStream and apply to RDD


```python
case class ClickStream(prev_id:String,curr_id:String,cnt:Integer,prev_title:String,curr_title:String,typ:String)
var clickRDDCase = clickRDD.map(x=> ClickStream(x._1,x._2,x._3,x._4,x._5,x._6));
clickRDDCase.take(2).foreach(println)
```


    ClickStream(,3632887,121,other-google,!!,other)
    ClickStream(,3632887,93,other-wikipedia,!!,other)



#### 2.3 Create a Dataframe from the RDD


```python
var clickDF = clickRDDCase.toDF();
clickDF.show()
```
```
+--------+--------+---+--------------------+-------------+-----+
| prev_id| curr_id|cnt|          prev_title|   curr_title|  typ|
+--------+--------+---+--------------------+-------------+-----+
|        | 3632887|121|        other-google|           !!|other|
|        | 3632887| 93|     other-wikipedia|           !!|other|
|        | 3632887| 46|         other-empty|           !!|other|
|        | 3632887| 10|         other-other|           !!|other|
|   64486| 3632887| 11|  !_(disambiguation)|           !!|other|
| 2061699| 2556962| 19|       Louden_Up_Now|  !!!_(album)| link|
|        | 2556962| 25|         other-empty|  !!!_(album)|other|
|        | 2556962| 16|        other-google|  !!!_(album)|other|
|        | 2556962| 44|     other-wikipedia|  !!!_(album)|other|
|   64486| 2556962| 15|  !_(disambiguation)|  !!!_(album)| link|
|  600744| 2556962|297|                 !!!|  !!!_(album)| link|
|        | 6893310| 11|         other-empty|!Hero_(album)|other|
| 1921683| 6893310| 26|               !Hero|!Hero_(album)| link|
|        | 6893310| 16|     other-wikipedia|!Hero_(album)|other|
|        | 6893310| 23|        other-google|!Hero_(album)|other|
| 8127304|22602473| 16|     Jericho_Rosales|   !Oka_Tokat| link|
|35978874|22602473| 20|List_of_telenovel...|   !Oka_Tokat| link|
|        |22602473| 57|        other-google|   !Oka_Tokat|other|
|        |22602473| 12|     other-wikipedia|   !Oka_Tokat|other|
|        |22602473| 23|         other-empty|   !Oka_Tokat|other|
+--------+--------+---+--------------------+-------------+-----+
only showing top 20 rows

```
### 3 Analyse what are the top searches leading to Wikipedia in Feb 2015

### 3.1 Analyse the Clickstream using Spark Dataframes

##### 3.1.1 Create a function to apply as a filter


```python
def fromSearchProvider(prevTitle:String) : Boolean = {
  val searchProviders = List("other-yahoo","other-bing","other-google")
  if (searchProviders.contains(prevTitle))
    {
      return true
    }
  else
    {
      return false
    }
}
```

##### 3.1.2 Convert function to an UDF to supply fromSearchProvider as filter function


```python
val search_filter = udf(fromSearchProvider _)
val clickSearch = clickDF.filter(search_filter($"prev_title"))
clickSearch.show()
```
```
+-------+--------+----+------------+--------------------+-----+
|prev_id| curr_id| cnt|  prev_title|          curr_title|  typ|
+-------+--------+----+------------+--------------------+-----+
|       | 3632887| 121|other-google|                  !!|other|
|       | 2556962|  16|other-google|         !!!_(album)|other|
|       | 6893310|  23|other-google|       !Hero_(album)|other|
|       |22602473|  57|other-google|          !Oka_Tokat|other|
|       | 6810768|  81|other-google|          !T.O.O.H.!|other|
|       |  899480|  17|other-google|          "A"_Device|other|
|       | 1282996|  10| other-yahoo|    "A"_Is_for_Alibi|other|
|       | 1282996| 272|other-google|    "A"_Is_for_Alibi|other|
|       | 9003666|  18|other-google|"And"_theory_of_c...|other|
|       |39072529|  49|other-google|"Bassy"_Bob_Brock...|other|
|       |25033979|  93|other-google|"C"_is_for_(Pleas...|other|
|       |  331586|6820|other-google|  "Crocodile"_Dundee|other|
|       |  331586| 274| other-yahoo|  "Crocodile"_Dundee|other|
|       |  331586| 417|  other-bing|  "Crocodile"_Dundee|other|
|       |16250593|  21|other-google| "D"_Is_for_Deadbeat|other|
|       |39304968| 108|other-google|"David_Hockney:_A...|other|
|       | 1896643|1227|other-google|"Dr._Death"_Steve...|other|
|       | 1896643|  70| other-yahoo|"Dr._Death"_Steve...|other|
|       | 1896643|  75|  other-bing|"Dr._Death"_Steve...|other|
|       |16251903|  26|other-google| "E"_Is_for_Evidence|other|
+-------+--------+----+------------+--------------------+-----+
only showing top 20 rows

```

##### 3.1.2 Find the top pages referred from search Engines


```python
val clickSearchVolume = clickSearch.groupBy(col("curr_title")).agg(sum(col("cnt")).alias("total_search")).orderBy($"total_search".desc)
clickSearchVolume.show()
```

```
+--------------------+------------+
|          curr_title|total_search|
+--------------------+------------+
|           Main_Page|     4171329|
|Fifty_Shades_of_Grey|     1903372|
|          Chris_Kyle|     1293055|
|    Alessandro_Volta|     1160284|
|     Stephen_Hawking|     1037257|
|    Better_Call_Saul|      989149|
|      Birdman_(film)|      982244|
|Fifty_Shades_of_G...|      877027|
|     Valentine's_Day|      831627|
| 87th_Academy_Awards|      794562|
|Islamic_State_of_...|      775541|
|    Chinese_New_Year|      740223|
|       Leonard_Nimoy|      683814|
|List_of_Bollywood...|      653926|
|        Bruce_Jenner|      629555|
|      Sia_(musician)|      618234|
|      Lunar_New_Year|      602595|
|      Dakota_Johnson|      598695|
|The_Walking_Dead_...|      581170|
|The_Flash_(2014_T...|      558487|
+--------------------+------------+
only showing top 20 rows

```

####  3.2 Analysis using Spark SQL

#### 3.2.1 Register DataFrame as a table


```python
clickDF.registerTempTable("wikiclickstream")
```


```python
clickSearchSQL = sqlContext.sql("select curr_title,sum(cnt) as total_search from wikiclickstream where prev_title in ('other-yahoo','other-bing','other-google') group by curr_title order by total_search desc")
clickSearchSQL.show()
```

```
+--------------------+------------+
|          curr_title|total_search|
+--------------------+------------+
|           Main_Page|     4171329|
|Fifty_Shades_of_Grey|     1903372|
|          Chris_Kyle|     1293055|
|    Alessandro_Volta|     1160284|
|     Stephen_Hawking|     1037257|
|    Better_Call_Saul|      989149|
|      Birdman_(film)|      982244|
|Fifty_Shades_of_G...|      877027|
|     Valentine's_Day|      831627|
| 87th_Academy_Awards|      794562|
|Islamic_State_of_...|      775541|
|    Chinese_New_Year|      740223|
|       Leonard_Nimoy|      683814|
|List_of_Bollywood...|      653926|
|        Bruce_Jenner|      629555|
|      Sia_(musician)|      618234|
|      Lunar_New_Year|      602595|
|      Dakota_Johnson|      598695|
|The_Walking_Dead_...|      581170|
|The_Flash_(2014_T...|      558487|
+--------------------+------------+
only showing top 20 rows
```

Why are these the top topics searched in Feb 2015

__Movies__
1. __Fifty Shades of Grey__ released in Feb 2015 featuring __Dakota Johnson__
2. __Birdman__ won 4 awards in __87th Academy Awards__

__People__
1. __Alessandro Volta__ is an Italian Physicist born on 14 Feb 1745. Google published a [Doodle](https://www.theguardian.com/science/the-h-word/2015/feb/18/alessandro-volta-anniversary-electricity-history-science), the potential reason for being one of the most searched Person in Feb 2015
2. __Chris Kyle__ a US Navy SEAL and sniper died in Feb
3. __Stephen Hawking__ attended an awards function for his biopic *Theory of Everything*
4. __Leonard_Nimoy__, the *Spock* of *Star Trek* died on 27 Feb 2015

__Events or Occassions__
1. __Chinese New year__ or __Lunar New Year__ is celebrated on 19 February 2015
2. __Valentines Day__ is on 14th February

###### *Note: Spark SQL code came out simple and elegant compared to DataFrame*

### 4 Analyse what are the top searches from Social Networks leading to Wikipedia in Feb 2015

#### 4.1 Analyse the Clickstream using Spark Dataframes

##### 4.1.1 Create a function to apply as a filter for social networks


```python
def fromSocialNetwork(prevTitle:String) : Boolean = {
  val socialProviders = List("other-twitter","other-facebook")
  if (socialProviders.contains(prevTitle))
    {
      return true
    }
  else
    {
      return false
    }
}
```

##### 4.1.2 Create an UDF to supply fromSocialNetwork as filter function


```python
val social_filter = udf(fromSocialNetwork _)
val clickSocial = clickDF.filter(social_filter($"prev_title"))
clickSocial.show()
```

```
+-------+--------+---+--------------+--------------------+-----+
|prev_id| curr_id|cnt|    prev_title|          curr_title|  typ|
+-------+--------+---+--------------+--------------------+-----+
|       |  331586| 20| other-twitter|  "Crocodile"_Dundee|other|
|       | 1261557| 13| other-twitter|            "Heroes"|other|
|       | 3564374| 33|other-facebook|     "I_AM"_Activity|other|
|       | 3564374| 24| other-twitter|     "I_AM"_Activity|other|
|       |18938265|406| other-twitter| "Weird_Al"_Yankovic|other|
|       |18938265| 33|other-facebook| "Weird_Al"_Yankovic|other|
|       | 7630017| 67| other-twitter|"Weird_Al"_Yankov...|other|
|       | 1578140| 13| other-twitter|                  %s|other|
|       |    2676| 16| other-twitter|    'Abd_al-Rahman_I|other|
|       |  430164| 13|other-facebook|        'Allo_'Allo!|other|
|       |  430164| 67| other-twitter|        'Allo_'Allo!|other|
|       |  175149| 36|other-facebook|        'Pataphysics|other|
|       |  175149| 96| other-twitter|        'Pataphysics|other|
|       | 1917971| 12| other-twitter|                  's|other|
|       |   50338| 16|other-facebook|    's-Hertogenbosch|other|
|       |   50338| 10| other-twitter|    's-Hertogenbosch|other|
|       |42995159| 30|other-facebook|  (357439)_2004_BL86|other|
|       |42995159| 24| other-twitter|  (357439)_2004_BL86|other|
|       | 1506853|172| other-twitter|(Don't_Fear)_The_...|other|
|       | 2448083| 16| other-twitter|(Everything_I_Do)...|other|
+-------+--------+---+--------------+--------------------+-----+
only showing top 20 rows

```

##### 4.1.3 Find the top pages referred from social network sites


```python
val socialVolume = clickSocial.groupBy(col("curr_title")).agg(sum(col("cnt")).alias("total_social")).orderBy($"total_social".desc)
socialVolume.show()
```

```
+--------------------+------------+
|          curr_title|total_social|
+--------------------+------------+
|    Johnny_Knoxville|      198976|
|      Peter_Woodcock|      126378|
|2002_Tampa_plane_...|      120955|
|      Sơn_Đoòng_Cave|      116126|
|       The_boy_Jones|      114524|
|             War_pig|      114138|
|William_Leonard_P...|      113906|
|Hurt_(Nine_Inch_N...|      103562|
|     Glass_recycling|       87995|
|Assassination_of_...|       86445|
|    Fury_(2014_film)|       80297|
|    Mullet_(haircut)|       73613|
|            Iron_Man|       69772|
|International_Mat...|       64475|
|Pirates_of_the_Ca...|       63517|
|            Asbestos|       62987|
|       Benjaman_Kyle|       61130|
|            New_Deal|       59854|
|     Bobbie_Joe_Long|       59836|
|        David_Reimer|       59136|
+--------------------+------------+
only showing top 20 rows

```


####  4.2 Analysis using Spark SQL


```python
val clickSocialSQL = sqlContext.sql("select curr_title,sum(cnt) as total_search from wikiclickstream where prev_title in ('other-twitter','other-facebook') group by curr_title order by total_search desc")
clickSocialSQL.show()
```

```
+--------------------+------------+
|          curr_title|total_search|
+--------------------+------------+
|    Johnny_Knoxville|      198976|
|      Peter_Woodcock|      126378|
|2002_Tampa_plane_...|      120955|
|      Sơn_Đoòng_Cave|      116126|
|       The_boy_Jones|      114524|
|             War_pig|      114138|
|William_Leonard_P...|      113906|
|Hurt_(Nine_Inch_N...|      103562|
|     Glass_recycling|       87995|
|Assassination_of_...|       86445|
|    Fury_(2014_film)|       80297|
|    Mullet_(haircut)|       73613|
|            Iron_Man|       69772|
|International_Mat...|       64475|
|Pirates_of_the_Ca...|       63517|
|            Asbestos|       62987|
|       Benjaman_Kyle|       61130|
|            New_Deal|       59854|
|     Bobbie_Joe_Long|       59836|
|        David_Reimer|       59136|
+--------------------+------------+

```
