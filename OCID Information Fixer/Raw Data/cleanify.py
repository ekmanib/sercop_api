import cleaning_funcs

for i in range(2016,2023):
    cc = attack(i)
    cc = [i.replace("\n","") for i in cc]
    content = "\n".join(cc)
    with open(f"Compras_Publicas_{i}\\Year_{i}_final.txt","w",encoding = "utf-8") as f:
        f.write(content)