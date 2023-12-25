-- Challenges
WITH cte AS(
    SELECT hacker_id
        , count(challenge_id) as total
    FROM challenges
    GROUP BY hacker_id
)
, cte2 AS(
    SELECT *
        , rank() OVER (ORDER BY total DESC) rank
        , COUNT(hacker_id) OVER (PARTITION BY total) num_hackerid
        FROM cte
)
SELECT c.hacker_id
    , h.name
    , total
FROM cte2 c
JOIN hackers h
ON c.hacker_id = h.hacker_id
WHERE num_hackerid =1 
    OR (num_hackerid <> 1 AND rank=1)
ORDER BY total desc, c.hacker_id 


-- The Report
SELECT 
    CASE WHEN Grade>= 8 THEN Name
    ELSE NULL
    END
    , Grade
    , Marks
FROM students s
LEFT JOIN Grades g
ON s.marks BETWEEN g.min_mark AND g.max_mark
ORDER BY grade DESC, name, marks ASC

-- Top Earners
SELECT
CONCAT(
    (SELECT MAX(salary*months) FROM Employee)
    ,'  '
    , (SELECT COUNT(employee_id)
        FROM Employee
        WHERE salary*months = (SELECT MAX(salary*months) FROM Employee))
)


-- The PADS
SELECT 
    CASE WHEN Occupation = 'Doctor' THEN CONCAT(Name, '(D)') 
    WHEN Occupation = 'Actor' THEN CONCAT(Name, '(A)') 
    WHEN Occupation = 'Singer' THEN CONCAT(Name, '(S)') 
    ELSE CONCAT(Name, '(P)') 
    END as n_o
FROM Occupations
-- ORDER BY n_o
UNION
SELECT 
    CONCAT('There are a total of ', total,' ', LOWER(Occupation),'s.')
FROM ( SELECT Occupation
        , count(name) as total
    FROM Occupations
    GROUP BY Occupation ) as cte
-- ORDER BY total, occupation


-- Ocupation
--cach 1
SELECT [Doctor], [Professor], [Singer], [Actor]
FROM 
    (SELECT *, row_number() OVER (PARTITION BY Occupation ORDER BY Name) rn 
     FROM Occupations
     ) as new_table
PIVOT
( Max(Name)
FOR
Occupation IN ([Doctor], [Professor], [Singer], [Actor])
) AS pivot_table

--cach 2
SELECT
    MAX(CASE WHEN Occupation = 'Doctor' THEN Name END) AS Doctor,
    MAX(CASE WHEN Occupation = 'Professor' THEN Name END) AS Professor,
    MAX(CASE WHEN Occupation = 'Singer' THEN Name END) AS Singer,
    MAX(CASE WHEN Occupation = 'Actor' THEN Name END) AS Actor
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY Occupation ORDER BY Name) as rn 
      FROM OCCUPATIONS) A
      GROUP BY rn


-- Contest Leaderboard
WITH cte AS(
    SELECT distinct h.*
        , challenge_id
        , MAX(score) OVER (PARTITION BY h.hacker_id, challenge_id) max
    FROM Hackers h
    JOIN Submissions s
    ON h.hacker_id = s.hacker_id
)
SELECT hacker_id   
    , name
    , sum(max) total
FROM cte
GROUP BY hacker_id, name
HAVING sum(max) <> 0
ORDER BY sum(max) desc, hacker


-- Interviews
WITH cte AS(
    SELECT c.contest_id, hacker_id, name
        , sum(ts) sta
        , sum(tas) stas
        , sum(tv) stv
        , sum(tuv) stuv
    FROM Contests c
    JOIN Colleges cl
    ON c.contest_id = cl.contest_id
    JOIN Challenges ch
    ON cl.college_id = ch.college_id
    -- order by hacker_id, cl.college_id, challenge_id
    LEFT JOIN (SELECT challenge_id 
                , sum(total_views) tv
                , sum(total_unique_views) tuv
          FROM View_Stats 
          GROUP BY challenge_id
         ) vs
    ON vs.challenge_id = ch.challenge_id
    LEFT JOIN (SELECT challenge_id 
                , sum(total_submissions) ts
                , sum(total_accepted_submissions) tas
          FROM Submission_Stats
          GROUP BY challenge_id
         ) ss
    ON ss.challenge_id = ch.challenge_id
    GROUP BY c.contest_id, hacker_id, name
)
SELECT * 
FROM cte
WHERE sta <> 0 and stas <> 0 and stv <> 0 and stuv <> 0
ORDER BY contest_id


-- SQL Project Planning
WITH start_pj AS(
    SELECT start_date 
        , row_number() OVER (ORDER BY start_date) sr
    FROM Projects
    WHERE start_date NOT IN (SELECT end_date FROM Projects)
)
, end_pj AS(
    SELECT end_date 
        , row_number() OVER (ORDER BY end_date) er
    FROM Projects 
    WHERE end_date NOT IN (SELECT start_date FROM Projects)
)
SELECT start_date
    , end_date
FROM start_pj s
JOIN end_pj e
ON s.sr = e.er
ORDER BY (DAY(end_date) - DAY(start_date)) 
    , start_date


-- 15 days of learning SQL
WITH total_hacker AS(
    SELECT submission_date
        , COUNT (DISTINCT hacker_id) total_hk
    FROM ( SELECT *  
            , DENSE_RANK() OVER (ORDER BY submission_date) date
            , DENSE_RANK() OVER (PARTITION BY hacker_id ORDER BY submission_date) rank_sub
            FROM Submissions) cte
    WHERE date = rank_sub
    GROUP BY submission_date
)
, hacker_max AS(
    SELECT *
        , MAX(total_sub) OVER (PARTITION BY submission_date) max_total_sub
        , min(hacker_id) OVER (PARTITION BY submission_date, total_sub) min_hacker_id
    FROM (SELECT distinct submission_date
                , hacker_id
                , COUNT(submission_id) as total_sub
            FROM Submissions
            GROUP BY submission_date, hacker_id) B
)
SELECT hx.submission_date
    , total_hk
    , hx.hacker_id
    , name
FROM hacker_max hx
JOIN total_hacker t ON t.submission_date = hx.submission_date
JOIN hackers h ON hx.hacker_id = h.hacker_id
WHERE max_total_sub = total_sub 
    AND hx.hacker_id = min_hacker_id
ORDER BY hx.submission_date


-- Olivander's Inventory
WITH cte AS(
    SELECT w.* 
        , age
        , min(coins_needed) OVER (PARTITION by power, age) AS min
    FROM Wands AS w 
    JOIN Wands_Property AS p 
    ON w.code = p.code
    WHERE is_evil = 0
    -- ORDER BY power DESC, age DESC
    )
SELECT id
    , age
    , coins_needed
    , power
FROM fact_table 
WHERE  coins_needed = min
ORDER BY power DESC, age DESC


-- Placements
SELECT Name
FROM (
SELECT Students.Name
    , Packages.Salary as salary_student
    , Friends.Friend_ID
FROM Students
JOIN Packages
ON Students.ID = Packages.ID
JOIN Friends
ON Students.ID = Friends.ID
    ) as fact_table
JOIN Packages
ON fact_table.Friend_ID = Packages.ID
WHERE salary_student < Salary
ORDER BY Salary 