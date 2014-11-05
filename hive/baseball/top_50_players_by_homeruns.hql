USE baseball_stats; 
SELECT m.playerID, m.nameFirst, m.nameLast, b.homeruns, b.yearID
  FROM batting b INNER JOIN master m ON b.playerID = m.playerID
  ORDER BY b.homeruns DESC
  LIMIT 50;