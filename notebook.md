<center><img src="MKn_Staffelter_Hof.jpeg" alt="Picture of old business"</center>
<!--Image Credit: Martin Kraft https://commons.wikimedia.org/wiki/File:MKn_Staffelter_Hof.jpg -->

Staffelter Hof Winery is Germany's oldest business, established in 862 under the Carolingian dynasty. It has continued to serve customers through dramatic changes in Europe, such as the Holy Roman Empire, the Ottoman Empire, and both world wars. What characteristics enable a business to stand the test of time?

To help answer this question, BusinessFinancing.co.uk researched the oldest company still in business in **almost** every country and compiled the results into several CSV files. This dataset has been cleaned.

Having useful information in different files is a common problem. While it's better to keep different types of data separate for data storage, you'll want all the data in one place for analysis. You'll use joining and data manipulation to work with this data and better understand the world's oldest businesses.

## The Data
`businesses` and `new_businesses`
|Column|Description|
|------|-----------|
|`business`|Name of the business (varchar)|
|`year_founded`|Year the business was founded (int)|
|`category_code`|Code for the business category (varchar)|
|`country_code`|ISO 3166-1 three-letter country code (char)|
---
`countries`
|Column|Description|
|------|-----------|
|`country_code`|ISO 3166-1 three-letter country code (varchar)|
|`country`|Name of the country (varchar)|
|`continent`|Name of the continent the country exists in (varchar)|
---
`categories`
|Column|Description|
|------|-----------|
|`category_code`|Code for the business category (varchar)|
|`category`|Description of the business category (varchar)|


```python
-- What is the oldest business on each continent?
WITH RankedBusinesses AS (
    SELECT 
        c.continent, 
        c.country, 
        b.business, 
        b.year_founded,
        ROW_NUMBER() OVER (PARTITION BY c.continent ORDER BY b.year_founded ASC) AS rank
    FROM 
        public.businesses b
    INNER JOIN 
        public.countries c
    ON 
        b.country_code = c.country_code
)
SELECT 
    continent, 
    country, 
    business, 
    year_founded
FROM 
    RankedBusinesses
WHERE 
    rank = 1
ORDER BY 
    continent;


```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>continent</th>
      <th>country</th>
      <th>business</th>
      <th>year_founded</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Africa</td>
      <td>Mauritius</td>
      <td>Mauritius Post</td>
      <td>1772</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Asia</td>
      <td>Japan</td>
      <td>Kongō Gumi</td>
      <td>578</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Europe</td>
      <td>Austria</td>
      <td>St. Peter Stifts Kulinarium</td>
      <td>803</td>
    </tr>
    <tr>
      <th>3</th>
      <td>North America</td>
      <td>Mexico</td>
      <td>La Casa de Moneda de México</td>
      <td>1534</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Oceania</td>
      <td>Australia</td>
      <td>Australia Post</td>
      <td>1809</td>
    </tr>
    <tr>
      <th>5</th>
      <td>South America</td>
      <td>Peru</td>
      <td>Casa Nacional de Moneda</td>
      <td>1565</td>
    </tr>
  </tbody>
</table>
</div>




```python
-- How many countries per continent lack data on the oldest businesses
-- Does including the `new_businesses` data change this?

-- Step 1: Create a list of all countries per continent
WITH AllCountries AS (
    SELECT 
        continent,
        country
    FROM 
        public.countries
),

-- Step 2: Create a list of countries with business data from both businesses and new_businesses
CountriesWithBusinessData AS (
    SELECT DISTINCT
        c.continent,
        c.country
    FROM 
        public.countries c
    INNER JOIN 
        public.businesses b
    ON 
        c.country_code = b.country_code

    UNION

    SELECT DISTINCT
        c.continent,
        c.country
    FROM 
        public.countries c
    INNER JOIN 
        public.new_businesses nb
    ON 
        c.country_code = nb.country_code
),

-- Step 3: Determine which countries lack business data
CountriesWithoutBusinessData AS (
    SELECT 
        a.continent,
        a.country
    FROM 
        AllCountries a
    LEFT JOIN 
        CountriesWithBusinessData b
    ON 
        a.country = b.country
    WHERE 
        b.country IS NULL
)

-- Step 4: Count the number of countries without business data for each continent
SELECT 
    continent,
    COUNT(country) AS countries_without_businesses
FROM 
    CountriesWithoutBusinessData
GROUP BY 
    continent
ORDER BY 
    continent;

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>continent</th>
      <th>countries_without_businesses</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Africa</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Asia</td>
      <td>7</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Europe</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>North America</td>
      <td>5</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Oceania</td>
      <td>10</td>
    </tr>
    <tr>
      <th>5</th>
      <td>South America</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>




```python
-- Which business categories are best suited to last over the course of centuries?
-- Step 1: Join the necessary tables to get the relevant data
WITH BusinessCategoryContinent AS (
    SELECT 
        c.continent,
        cat.category,
        b.year_founded
    FROM 
        public.businesses b
    INNER JOIN 
        public.countries c
    ON 
        b.country_code = c.country_code
    INNER JOIN 
        public.categories cat
    ON 
        b.category_code = cat.category_code

)

-- Step 2: Get the oldest founding year for each continent and category combination
SELECT 
    continent,
    category,
    MIN(year_founded) AS year_founded
FROM 
    BusinessCategoryContinent
GROUP BY 
    continent, 
    category
ORDER BY 
    continent, 
    category;

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>continent</th>
      <th>category</th>
      <th>year_founded</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Africa</td>
      <td>Agriculture</td>
      <td>1947</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Africa</td>
      <td>Aviation &amp; Transport</td>
      <td>1854</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Africa</td>
      <td>Banking &amp; Finance</td>
      <td>1892</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Africa</td>
      <td>Distillers, Vintners, &amp; Breweries</td>
      <td>1933</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Africa</td>
      <td>Energy</td>
      <td>1968</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Africa</td>
      <td>Food &amp; Beverages</td>
      <td>1878</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Africa</td>
      <td>Manufacturing &amp; Production</td>
      <td>1820</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Africa</td>
      <td>Media</td>
      <td>1943</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Africa</td>
      <td>Mining</td>
      <td>1962</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Africa</td>
      <td>Postal Service</td>
      <td>1772</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Asia</td>
      <td>Agriculture</td>
      <td>1930</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Asia</td>
      <td>Aviation &amp; Transport</td>
      <td>1858</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Asia</td>
      <td>Banking &amp; Finance</td>
      <td>1830</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Asia</td>
      <td>Cafés, Restaurants &amp; Bars</td>
      <td>1153</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Asia</td>
      <td>Conglomerate</td>
      <td>1841</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Asia</td>
      <td>Construction</td>
      <td>578</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Asia</td>
      <td>Defense</td>
      <td>1808</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Asia</td>
      <td>Distillers, Vintners, &amp; Breweries</td>
      <td>1853</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Asia</td>
      <td>Energy</td>
      <td>1928</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Asia</td>
      <td>Food &amp; Beverages</td>
      <td>1820</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Asia</td>
      <td>Manufacturing &amp; Production</td>
      <td>1736</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Asia</td>
      <td>Media</td>
      <td>1931</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Asia</td>
      <td>Mining</td>
      <td>1913</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Asia</td>
      <td>Postal Service</td>
      <td>1800</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Asia</td>
      <td>Retail</td>
      <td>1883</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Asia</td>
      <td>Telecommunications</td>
      <td>1885</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Asia</td>
      <td>Tourism &amp; Hotels</td>
      <td>1584</td>
    </tr>
    <tr>
      <th>27</th>
      <td>Europe</td>
      <td>Agriculture</td>
      <td>1218</td>
    </tr>
    <tr>
      <th>28</th>
      <td>Europe</td>
      <td>Banking &amp; Finance</td>
      <td>1606</td>
    </tr>
    <tr>
      <th>29</th>
      <td>Europe</td>
      <td>Cafés, Restaurants &amp; Bars</td>
      <td>803</td>
    </tr>
    <tr>
      <th>30</th>
      <td>Europe</td>
      <td>Consumer Goods</td>
      <td>1649</td>
    </tr>
    <tr>
      <th>31</th>
      <td>Europe</td>
      <td>Defense</td>
      <td>1878</td>
    </tr>
    <tr>
      <th>32</th>
      <td>Europe</td>
      <td>Distillers, Vintners, &amp; Breweries</td>
      <td>862</td>
    </tr>
    <tr>
      <th>33</th>
      <td>Europe</td>
      <td>Manufacturing &amp; Production</td>
      <td>864</td>
    </tr>
    <tr>
      <th>34</th>
      <td>Europe</td>
      <td>Media</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>35</th>
      <td>Europe</td>
      <td>Medical</td>
      <td>1422</td>
    </tr>
    <tr>
      <th>36</th>
      <td>Europe</td>
      <td>Mining</td>
      <td>1248</td>
    </tr>
    <tr>
      <th>37</th>
      <td>Europe</td>
      <td>Postal Service</td>
      <td>1520</td>
    </tr>
    <tr>
      <th>38</th>
      <td>Europe</td>
      <td>Telecommunications</td>
      <td>1912</td>
    </tr>
    <tr>
      <th>39</th>
      <td>Europe</td>
      <td>Tourism &amp; Hotels</td>
      <td>1230</td>
    </tr>
    <tr>
      <th>40</th>
      <td>North America</td>
      <td>Agriculture</td>
      <td>1638</td>
    </tr>
    <tr>
      <th>41</th>
      <td>North America</td>
      <td>Aviation &amp; Transport</td>
      <td>1870</td>
    </tr>
    <tr>
      <th>42</th>
      <td>North America</td>
      <td>Banking &amp; Finance</td>
      <td>1891</td>
    </tr>
    <tr>
      <th>43</th>
      <td>North America</td>
      <td>Distillers, Vintners, &amp; Breweries</td>
      <td>1703</td>
    </tr>
    <tr>
      <th>44</th>
      <td>North America</td>
      <td>Food &amp; Beverages</td>
      <td>1920</td>
    </tr>
    <tr>
      <th>45</th>
      <td>North America</td>
      <td>Manufacturing &amp; Production</td>
      <td>1534</td>
    </tr>
    <tr>
      <th>46</th>
      <td>North America</td>
      <td>Media</td>
      <td>1909</td>
    </tr>
    <tr>
      <th>47</th>
      <td>North America</td>
      <td>Retail</td>
      <td>1670</td>
    </tr>
    <tr>
      <th>48</th>
      <td>North America</td>
      <td>Tourism &amp; Hotels</td>
      <td>1770</td>
    </tr>
    <tr>
      <th>49</th>
      <td>Oceania</td>
      <td>Banking &amp; Finance</td>
      <td>1861</td>
    </tr>
    <tr>
      <th>50</th>
      <td>Oceania</td>
      <td>Postal Service</td>
      <td>1809</td>
    </tr>
    <tr>
      <th>51</th>
      <td>South America</td>
      <td>Banking &amp; Finance</td>
      <td>1565</td>
    </tr>
    <tr>
      <th>52</th>
      <td>South America</td>
      <td>Cafés, Restaurants &amp; Bars</td>
      <td>1877</td>
    </tr>
    <tr>
      <th>53</th>
      <td>South America</td>
      <td>Defense</td>
      <td>1811</td>
    </tr>
    <tr>
      <th>54</th>
      <td>South America</td>
      <td>Food &amp; Beverages</td>
      <td>1660</td>
    </tr>
    <tr>
      <th>55</th>
      <td>South America</td>
      <td>Manufacturing &amp; Production</td>
      <td>1621</td>
    </tr>
  </tbody>
</table>
</div>


