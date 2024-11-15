let

    // Cria a tabela horarios
    horarios = #table(

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
            PostMeridiem = text
        ],
        
        // Conteúdo dos dados
        List.Transform(
            {0..86399},  //Indice geral em lista
            each  let 
                horarioRecord = Duration.ToRecord(#duration(0, 0, 0, _)),  // Horário em record
                hora = horarioRecord[Hours], 
                minuto = horarioRecord[Minutes],
                segundo = horarioRecord[Seconds],
                zws = Character.FromNumber(8203) // Caracter especial para ordenação
            in {
                _, // Indice
                _/3600, //HorarioDecimal
                #time(hora, minuto, segundo), // Horario
                #time(hora, 0, 0), // InicioHora
                #time(hora, minuto, 0), // InicioMinuto
                hora,
                minuto,
                segundo,
                
                // Periodo
                if hora < 6 then Text.Repeat(zws, 3) & "Madrugada" 
                    else if hora < 12 then Text.Repeat(zws, 2) & "Manhã" 
                    else if hora < 18 then Text.Repeat(zws, 1) &  "Tarde" 
                    else "Noite", 
                
                if hora < 12 then "AM" else "PM" // PostMeridiem
                
            }
        )
    ),

    // Definição dos turnos, horários iniciais e finais
    turnos = Table.FromRows(
        List.Zip({
            { "Turno 1"        , "Turno 2"        , "Turno 3"         }, // Tag dos turnos
            { #time(05, 00, 00), #time(13, 30, 00), #time(22, 00, 00) }, // Horário inicial
            { #time(13, 29, 59), #time(21, 59, 59), #time(04, 59, 59) }  // Horário final
        }), 
        {"Turno", "HorarioInicial", "HorarioFinal"}
    ),
    
    // Turno especial quando o horário final é menor que o inicial 
    turnoEspecial = Table.SelectRows(turnos, each [HorarioFinal] < [HorarioInicial])[Turno]{0},
    
    // Adiciona o turno a tabela horarios
    turnoAdicionado = Table.AddColumn(
        horarios, 
        "Turno", 
        (h) => 
            try 
                Table.SelectRows(
                    turnoEspecial, 
                    (t) => h[Horario] >= t[HorarioInicial] and h[Horario] <= t[HorarioFinal])[Turno]{0} 
            otherwise 
                turnoEspecial, 
        type text
    )

in
    turnoAdicionado
