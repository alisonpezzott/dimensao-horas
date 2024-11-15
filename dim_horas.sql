DROP TABLE IF EXISTS dim_horas;

-- Nomes e tipos das colunas
CREATE TABLE dim_horas (
    Indice INT,
    HorarioDecimal DECIMAL(18, 8),
    Horario TIME,
    InicioHora TIME,
    InicioMinuto TIME,
    Hora INT,
    Minuto INT,
    Segundo INT,
    Periodo VARCHAR(10),
    PostMeridiem CHAR(2),
    Turno VARCHAR(10)
);

-- Tabela temporária com os segundos do dia
IF OBJECT_ID('tempdb..#temp') IS NOT NULL
    DROP TABLE #temp;
CREATE TABLE #temp (Indice INT);

-- 86400 segundos em um dia
INSERT INTO #temp (Indice)
SELECT TOP (86400) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS Indice
FROM sys.all_objects a
CROSS JOIN sys.all_objects b;

-- Primeira CTE para reutilização
WITH tb_primaria AS (
    SELECT
        Indice,
        CAST(Indice / 3600.0 AS DECIMAL(18, 8)) AS HorarioDecimal,
        DATEADD(SECOND, Indice, CAST('00:00:00' AS TIME)) AS Horario
    FROM #temp
),

-- Segunda CTE para definir as demais colunas
tabela_completa AS (
    SELECT 
        Indice,
        HorarioDecimal,
        Horario,
        
        -- Calculando InicioHora e InicioMinuto com base em Horario
        CAST(DATEADD(HOUR, DATEPART(HOUR, Horario), '00:00:00') AS TIME) AS InicioHora,
        CAST(DATEADD(MINUTE, DATEPART(MINUTE, Horario), DATEADD(HOUR, DATEPART(HOUR, Horario), '00:00:00')) AS TIME) AS InicioMinuto,

        -- Extraindo Hora, Minuto e Segundo
        DATEPART(HOUR, Horario) AS Hora,
        DATEPART(MINUTE, Horario) AS Minuto,
        DATEPART(SECOND, Horario) AS Segundo,

        -- Periodo
        CASE 
            WHEN DATEPART(HOUR, Horario) < 6 THEN 'Madrugada'
            WHEN DATEPART(HOUR, Horario) < 12 THEN 'Manhã'
            WHEN DATEPART(HOUR, Horario) < 18 THEN 'Tarde'
            ELSE 'Noite'
        END AS Periodo,
        
        -- PostMeridiem
        IIF(DATEPART(HOUR, Horario) < 12, 'AM', 'PM') AS PostMeridiem,
        
        -- Turno baseado em horários específicos, usando a coluna Horario já calculada
        CASE 
            WHEN Horario >= '05:00:00' AND Horario <= '13:29:59' THEN 'Turno 1'
            WHEN Horario >= '13:30:00' AND Horario <= '21:59:59' THEN 'Turno 2'
            WHEN Horario >= '22:00:00' OR Horario <= '04:59:59' THEN 'Turno 3'
            ELSE NULL
        END AS Turno
    FROM tb_primaria
)

-- Insere dados da segunda CTE na dim_horas
INSERT INTO dim_horas
SELECT * FROM tabela_completa
ORDER BY Indice;

-- Apaga a tabela temporária
DROP TABLE #temp;

-- Exibi os dados inseridos
SELECT * FROM dim_horas ORDER BY Indice;
