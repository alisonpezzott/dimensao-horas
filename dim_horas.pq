let

    dim_horas = #table(

        // Nomes e tipos das colunas   
        type table[   
            Indice = Int64.Type,
            Horario24 = time,
            Horario12 = text,
            HoraInicio = time,
            MinutoInicio = time,
            Hora = Int64.Type,
            Minuto = Int64.Type,
            Segundo = Int64.Type,
            Periodo = text,
            Turno = text
        ],
        
        // Conteúdo dos dados
        List.Transform(
            // 86400 segundos em um dia
            {0..86399},

            // Variáveis para reutilização
            (i) =>  let 
                horarioRecord = Duration.ToRecord(#duration(0, 0, 0, i)),  // Horário em record
                hora = horarioRecord[Hours], 
                minuto = horarioRecord[Minutes],
                segundo = horarioRecord[Seconds],
                horario = #time(hora, minuto, segundo),
                zws = Character.FromNumber(8203) // Caracter especial para ordenação
            in 
            
            // Saída dos dados
            {
                i, 
                horario, 
                Time.ToText(horario, [Format = "hh:mm:ss tt", Culture = "en-US"]),
                #time(hora, 0, 0), 
                #time(hora, minuto, 0),
                hora,
                minuto,
                segundo,
                
                // Periodo
                if hora < 6  then Text.Repeat(zws, 3) & "Madrugada" else 
                if hora < 12 then Text.Repeat(zws, 2) & "Manhã"     else 
                if hora < 18 then Text.Repeat(zws, 1) & "Tarde"     else "Noite", 
                
                // Turno
                if horario >= #time(05, 00, 00) and horario <= #time(13, 29, 59) then "T1" else
                if horario >= #time(13, 30, 00) and horario <= #time(21, 59, 59) then "T2" else
                if horario >= #time(22, 00, 00) or  horario <= #time(04, 59, 59) then "T3" else null
                
            }
        )
    )
in
    dim_horas