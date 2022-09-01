Sub RemoveDups()
    Dim r As Range, s As String, arr
    Dim c As Collection

    For Each r In Selection
        Set c = New Collection
        arr = Split(r.Value, " ")  REM add deleimeter
        For i = LBound(arr) To UBound(arr)
            On Error Resume Next
                c.Add arr(i), CStr(arr(i))
            On Error GoTo 0
        Next i

        s = ""
        For i = 1 To c.Count
            s = s & " " & c.Item(i)
        Next i
        If Left(s, 1) = " " Then s = Mid(s, 2)
        r.Value = s
    Next r
End Sub


