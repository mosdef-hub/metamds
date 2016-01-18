import os
import metamds as mds


def test_lysozyme_tutorial():
    tutorial = mds.Simulation(name='lysozyme')

    pdb2gmx = 'echo 6 | gmx pdb2gmx -f 1AKI.pdb -o 1AKI_processed.gro -water spce'
    editconf = 'gmx editconf -f 1AKI_processed.gro -o 1AKI_newbox.gro -c -d 1.0 -bt cubic'
    solvate = 'gmx solvate -cp 1AKI_newbox.gro -cs spc216.gro -o 1AKI_solv.gro -p topol.top'
    ion_grompp = 'gmx grompp -f ions.mdp -c 1AKI_solv.gro -p topol.top -o ions.tpr'
    genion = 'echo 13 | gmx genion -s ions.tpr -o 1AKI_solv_ions.gro -p topol.top -pname NA -nname CL -nn 8'

    script = (pdb2gmx, editconf, solvate, ion_grompp, genion)
    build = mds.Task(name='build', project=tutorial, script=script, input_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)),'lysozyme'))

    em_grompp = 'gmx grompp -f minim.mdp -c 1AKI_solv_ions.gro -p topol.top -o em.tpr'
    em_mdrun = 'gmx mdrun -v -deffnm em'

    script = (em_grompp, em_mdrun)
    minimize = mds.Task(name='minimize', project=tutorial, script=script, input_dir='lysozyme')

    tutorial.add_task(build)
    tutorial.add_task(minimize)
    tutorial.execute()
    print(tutorial.dir)
