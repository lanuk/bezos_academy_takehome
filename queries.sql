CREATE EXTERNAL TABLE IF NOT EXISTS enrollment_data (
    year INT,
    ncessch STRING,
    ncessch_num BIGINT,
    grade INT,
    race INT,
    sex INT,
    enrollment INT,
    fips INT,
    leaid STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION 's3://bezos-academy-prek-enrollment/data/';

CREATE EXTERNAL TABLE IF NOT EXISTS fips_mapping (
    fips INT,
    state STRING
)
LOCATION 's3://bezos-academy-prek-enrollment/fips_mapping';

INSERT INTO fips_mapping
VALUES
(1,'Alabama'),
(2,'Alaska'),
(3,'American Samoa'),
(4,'Arizona'),
(5,'Arkansas'),
(6,'California'),
(7,'Canal Zone'),
(8,'Colorado'),
(9,'Connecticut'),
(10,'Delaware'),
(11,'District of Columbia'),
(12,'Florida'),
(13,'Georgia'),
(14,'Guam'),
(15,'Hawaii'),
(16,'Idaho'),
(17,'Illinois'),
(18,'Indiana'),
(19,'Iowa'),
(20,'Kansas'),
(21,'Kentucky'),
(22,'Louisiana'),
(23,'Maine'),
(24,'Maryland'),
(25,'Massachusetts'),
(26,'Michigan'),
(27,'Minnesota'),
(28,'Mississippi'),
(29,'Missouri'),
(30,'Montana'),
(31,'Nebraska'),
(32,'Nevada'),
(33,'New Hampshire'),
(34,'New Jersey'),
(35,'New Mexico'),
(36,'New York'),
(37,'North Carolina'),
(38,'North Dakota'),
(39,'Ohio'),
(40,'Oklahoma'),
(41,'Oregon'),
(42,'Pennsylvania'),
(43,'Puerto Rico'),
(44,'Rhode Island'),
(45,'South Carolina'),
(46,'South Dakota'),
(47,'Tennessee'),
(48,'Texas'),
(49,'Utah'),
(50,'Vermont'),
(51,'Virginia'),
(52,'Virgin Islands of the US'),
(53,'Washington'),
(54,'West Virginia'),
(55,'Wisconsin'),
(56,'Wyoming'),
(58,'Department of Defense Dependent Schools (overseas)'),
(59,'Bureau of Indian Education'),
(60,'American Samoa'),
(61,'Department of Defense Dependent Schools (domestic)'),
(63,'Department of Defense Education Activity'),
(64,'Federated States of Micronesia'),
(65,'Mariana Islands waters (including Guam)'),
(66,'Guam'),
(67,'Johnston Atoll'),
(68,'Marshall Islands'),
(69,'Northern Mariana Islands'),
(70,'Palau'),
(71,'Midway Islands'),
(72,'Puerto Rico'),
(74,'US Minor Outlying Islands'),
(75,'Atlantic coast from North Carolina to Florida, and the coasts of Puerto Rico and Virgin Islands'),
(76,'Navassa Island'),
(78,'Virgin Islands of the US'),
(79,'Wake Island'),
(81,'Baker Island'),
(84,'Howland Island'),
(86,'Jarvis Island'),
(89,'Kingman Reef'),
(95,'Palmyra Atoll'),
(-1,'Missing/not reported'),
(-2,'Not applicable'),
(-3,'Suppressed data')
;

/* 10 states with highest number of children enrolled in Pre-K in 2021:
    1. Texas        6. Georgia
    2. Illinois     7. New Jersey
    3. Florida      8. Ohio
    4. New York     9. Oklahoma
    5. Wisconsin    10. Virginia
*/
SELECT
    f.state,
    SUM(e.enrollment) AS enrollment
FROM
    enrollment_data e
LEFT JOIN
    fips_mapping f ON e.fips = f.fips
WHERE
    e.year = 2021
    AND (
        (f.fips NOT IN (3, 7, 11, 14, 43, 52))
        AND
        (f.fips BETWEEN 1 AND 56)
    ) 
GROUP BY
    f.state
ORDER BY
    SUM(e.enrollment) DESC
LIMIT 10;