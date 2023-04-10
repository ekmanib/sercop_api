d = ["Utilidad_Neta","Activos_Totales","Patrimonio_Neto","Total_Ingresos"]
with open("renamer.txt","w") as f:
	for i in range(len(b)):
		print(poyo)
		doc = str(c[i])
		doc2 = doc.remove("use ","")
		varis = str(b[i]).replace(" NONE","")
		index = d[:len(varis.split(" "))] 
		f.write("clear\n")
		f.write(doc)
		f.write(f"rename ({varis}) ({index})")



