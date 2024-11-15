let
    data_inicial = #date(2022, 1, 1),
    data_final = DateTime.Date(DateTime.LocalNow()),
    datas = List.Generate(
        () => [Data = data_inicial],
        each [Data] <= data_final,
        each [Data = Date.AddDays([Data], 1)]
    ),
    calendario = #table(
        type table[
            Data = date,
            Ano = number,
            MesNome = text,
            MesNumero = number
        ],
        List.Transform(
            datas,
            (d) => {
                d,
                Date.Year(d),
                Date.MonthName(d),
                Date.Month(d)
            }
        )
    )
in
    calendario