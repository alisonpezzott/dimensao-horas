let

    dim_horas = #table(

        // Nomes e tipos de colunas   
        type table[   
            Indice = Int64.Type,
            HorarioDecimal = number,
            Horario = time,
            InicioHora = time,
            InicioMinuto = time,
            Hora = Int64.Type,
            Minuto = Int64.Type,
            Segundo = Int64.Type,
            Periodo = text,
            PostMeridiem = text,
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
                i/3600, 
                horario, 
                #time(hora, 0, 0), 
                #time(hora, minuto, 0),
                hora,
                minuto,
                segundo,
                
                // Periodo
                if hora < 6 then Text.Repeat(zws, 3) & "Madrugada" else 
                if hora < 12 then Text.Repeat(zws, 2) & "Manhã" else 
                if hora < 18 then Text.Repeat(zws, 1) &  "Tarde" else "Noite", 
                
                // PostMeridiem,
                if hora < 12 then "AM" else "PM", 

                // Turno
                if horario >= #time(05, 00, 00) and horario <= #time(13, 29, 59) then "Turno 1" else
                if horario >= #time(13, 30, 00) and horario <= #time(21, 59, 59) then "Turno 2" else
                if horario >= #time(22, 00, 00) or  horario <= #time(04, 59, 59) then "Turno 3" else null
                
            }
        )
    )
in
    dim_horas