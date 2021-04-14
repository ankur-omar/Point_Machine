from tkinter import OptionMenu, Label, StringVar, Frame, Tk
from tkinter import *

# Create object
root = Tk()

# Adjust size
root.geometry("200x200")


# Change the label text
def show():
    label.config(text=clicked.get())


# Dropdown menu options
options =['201A','201B','202A','202B','203A','203B','204A','204B','205A','205B','207A','207B','292A','292B','294',
           '295A','295B','296A','296B','297A','297B','299A','299B']

# datatype of menu text
clicked = StringVar()

# initial menu text
clicked.set("Point Name")

# Create Dropdown menu
drop = OptionMenu(root, clicked, *options)
drop.pack()

# Create button, it will change label text
button = Button(root, text="click Me", command=show).pack()

# Create Label
label = Label(root, text=" ")
label.pack()

# Execute tkinter
root.mainloop()